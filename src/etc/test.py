import requests
import time
import random
import sys
import concurrent.futures
import json
import subprocess
import shutil
import os

#configuration
DIRECT_SERVERS = [f"http://localhost:{port}" for port in range(8080, 8092)]
GATEWAY_URL = "http://localhost/api"
DOCKER_COMPOSE_FILE = os.path.join("..", "docker", "docker-compose.yaml")

# colors for output
HEADER = "\033[95m"
OKGREEN = "\033[92m"
FAIL = "\033[91m"
WARNING = "\033[93m"
ENDC = "\033[0m"

#Test status
def log(msg): print(f"{HEADER}[TEST]{ENDC} {msg}")

def success(msg): print(f"{OKGREEN}[PASS]{ENDC} {msg}")

def warn(msg): print(f"{WARNING}[WARN]{ENDC} {msg}")

def fail(msg): 
    print(f"{FAIL}[FAIL]{ENDC} {msg}")
    raise Exception(msg)

# global 
CREATOR_ID = None
BIDDER_ID = None
ITEM_ID = None

#helper func to stop or start a server through docker
def run_docker_command(action, service_name=None):
    #if stop kill the server
    if action == "stop": action = "kill"
    #goes to docker
    base_cmd = ["docker", "compose"] if shutil.which("docker-compose") is None else ["docker-compose"]
    cmd = base_cmd + ["-f", DOCKER_COMPOSE_FILE, action]
    if service_name:
        cmd.append(service_name)
    try:
        #run the command
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError as err:
        print(f"{FAIL}[DOCKER ERROR]{ENDC} failed to {action} {service_name}: {erreq.stderr}")
        return False

#start all nodes
def recover_all_nodes():

    print(f"\n{WARNING}[CLEANUP]{ENDC} ensureing all nodes are activates")
    run_docker_command("start")

#finds who is the leader
def find_leader():
    #goes to servers and get information from api
    for url in DIRECT_SERVERS:
        try:
            req = requests.get(f"{url}/info", timeout=1)

            if "Leader:true" in req.text:
                port = int(url.split(":")[-1])

                return port - 8080, url
            
        except:
            continue
        
    return None, None

# wait for the cluster to boot before starting test
def wait_for_cluster():

    print("waiting for cluster to boot", end="", flush=True)

    # 60 seconds timeout
    for _ in range(60):
        alive = 0

        for url in DIRECT_SERVERS:

            try:
                requests.get(f"{url}/info", timeout=0.5)
                alive += 1
            except:
                pass
        
        if alive >= 7:
            print(f" system ready, {alive} nodes online")
            return
            
        time.sleep(1)
        print(".", end="", flush=True)

    fail("cluster failed to start")



##  Test 1 - basic read and write
def test_basic_flow():
    global CREATOR_ID, BIDDER_ID, ITEM_ID
    log("Testing registration, auction creation and reading")
    
    # register 1st user
    try:
        req = requests.post(DIRECT_SERVERS[0] + "/register")
        if req.status_code != 200: fail(f"registration failed: {req.text}")

        CREATOR_ID = req.json()['user_id']
        success(f"registered creator id: {CREATOR_ID}")

    except Exception as err: fail(str(err))

    # register 2nd user
    try:
        req = requests.post(DIRECT_SERVERS[0] + "/register")
        if req.status_code != 200: fail(f"registration failed: {req.text}")
        BIDDER_ID = req.json()['user_id']
        success(f"registered bidder id: {BIDDER_ID}")
    except Exception as err: fail(str(err))

    # 1st user create auction
    try:
        payload = {"user_id": CREATOR_ID, "item_name": "TestCouch", "amount": 100}
        req = requests.post(DIRECT_SERVERS[0] + "/create", json=payload)
        ITEM_ID = req.json().get('item_id')
        success(f"created item id: {ITEM_ID}")
    except Exception as err: fail(str(err))

    # 2nd user try to find the auction of 1st user
    time.sleep(1) 
    target = DIRECT_SERVERS[-1] 
    try:
        req = requests.get(f"{target}/status/item_id?item_id={ITEM_ID}")
        if req.json().get('item_name') == "TestCouch":
            success(f"replication verified, server {target} sees 'TestCouch'")
        else:
            fail(f"Server {target} returned wrong data: {req.text}")
    except: fail(f"Server {target} unreachable or empty")




## Test 2 - Batching
def test_batching():
    #global variable
    global CREATOR_ID

    log("Testing linearizable batch requests")
    #if the user hasnt registered yet, we will register it
    if CREATOR_ID is None:
        try:
            req = requests.post(DIRECT_SERVERS[0] + "/register")
            if req.status_code == 200:
                CREATOR_ID = req.json()['user_id']
                success(f"registered user for batching: {CREATOR_ID}")
            else:
                fail(f"fail to register user for batch test: {req.text}")
        except Exception as err:
            fail(f"registration failed: {err}")
    
    #batch request
    batch_request = [
        {"type": "CREATE_AUCTION", "item_name": "batchitem1", "amount": 10, "user_id": CREATOR_ID},
        {"type": "CREATE_AUCTION", "item_name": "batchitem2", "amount": 20, "user_id": CREATOR_ID},
        {"type": "CREATE_AUCTION", "item_name": "batchitem3", "amount": 30, "user_id": CREATOR_ID}
    ]
    
    #submit batch request
    try:
        req = requests.post(DIRECT_SERVERS[0] + "/batch", json=batch_request)
        data = req.json()
        if 'error' in data:
            fail(f"batch rejected because: {data['error']}")
        results = req.json().get('batch_results', [])
        if len(results) == 3:
            success(f"batch processed {len(results)} commands successfully")
        else:
            fail(f"batch results invalid: {results}")

    except Exception as err: fail(str(err))





## Test 3 - consistency

def test_consistency():

    log("Testing linearizable read")
    #bid
    bid_payload = {"type": "PLACE_BID", "item_id": ITEM_ID, "amount": 500, "user_id": BIDDER_ID}
    
    req = requests.post(DIRECT_SERVERS[0] + "/bid", json=bid_payload)
    if req.status_code != 200: fail(f"Bid failed: {req.text}")

    # linearizable read from server 1
    start = time.time()
    req = requests.get(f"{DIRECT_SERVERS[1]}/status/bid/sync?id={ITEM_ID}")
    duration = time.time() - start
    
    try:
        data = req.json()
        if data.get('amount') == 500:
            success(f"linearizable read (/sync) returned correct bid: 500, it took {duration:.3f}s")
        else:
            fail(f"L=linearizable read returned wrong data, got: {data}")

    except Exception as err: fail(f"read failed: {err}")




# Test 4 - Leader failure and recovery
def test_leader_failure():
    log("\n Testing leader failure and failover")
    
    # identify Leader
    leader_id, leader_url = find_leader()
    if leader_id is None: fail("could not find a leader")
    print(f"leader is server {leader_id} ({leader_url})")

    # kill the leader
    print(f"killing the leader, killing server {leader_id}")
    run_docker_command("stop", f"server{leader_id}")
    time.sleep(2) 

    # attempt to write 
    alive_node = (leader_id + 1) % 12
    target_url = DIRECT_SERVERS[alive_node]
    print(f"sending write request to alive server: {alive_node}")
    
    # retry loop to let the election end
    start = time.time()
    success_write = False

    while time.time() - start < 15:
        try:
            payload = {"type": "CREATE_AUCTION", "item_name": "SurvivalItem", "amount": 99, "user_id": CREATOR_ID}
            req = requests.post(f"{target_url}/create", json=payload, timeout=2)
            if req.status_code == 200:
                success_write = True
                break
        except:
            pass
        time.sleep(0.5)

    #if we wrote - the system recovered
    if success_write:
        success("system recovered. Write accepted after leader failure")
    else:
        fail("system failed to elect new leader or accept writes")

    # recover old leader
    print(f"resurrecting server {leader_id}")
    run_docker_command("start", f"server{leader_id}")
    success(f"Server {leader_id} restarted.")
    time.sleep(5) 







## Test 5 - follower failure and catchup using log/snapshots
def test_follower_recovery():
    log("\nTesting follower failure and catchup using log/snapshots")
    
    # find a follower 
    curr_leader, _ = find_leader()
    victim_id = (curr_leader + 1) % 12
    print(f"victim selected: follower server {victim_id}")

    # kill victim
    run_docker_command("stop", f"server{victim_id}")
    print(f"server {victim_id} is offline")

    # make changes to the cluster while he is dead
    print("writing data to cluster withougt the follower")
    for i in range(5):

        requests.post(GATEWAY_URL + "/create", json={
            "type": "CREATE_AUCTION", "item_name": f"missedItem{i}", "amount": 10, "user_id": CREATOR_ID
        })
    
    # restart follower
    print(f"resurrecting server {victim_id}")
    run_docker_command("start", f"server{victim_id}")
    
    # wait for Catch-up
    print("waiting for sync")
    time.sleep(10) 

    # verify Data
    try:
        req = requests.get(f"{DIRECT_SERVERS[victim_id]}/status/name?name=missedItem4")
        data = req.json()

        if len(data) > 0 and data[0]['item_name'] == "missedItem4":
            success(f"recovery verified, server {victim_id} caught up and has 'missedItem4'")
        else:
            fail(f"server {victim_id} did not recover data, got: {data}")

    except Exception as err:
        warn(f"verification failed: {err}")




## Test 6 - load test -  1000 clients at the same time
def test_load():
    log("\nstarting 1000 clients - load test")
    print(f"targeting Gateway: {GATEWAY_URL}")

    CONCURRENT_CLIENTS = 50
    TOTAL_REQS = 1000
    
    #all clients must register
    def client_task(i):
        try:
            req = requests.post(GATEWAY_URL + "/register", timeout=5)
            return req.status_code
        except: return 503

    start_time = time.time()
    results = []
    
    #all clients make the action together
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_CLIENTS) as executor:
        futures = [executor.submit(client_task, i) for i in range(TOTAL_REQS)]
        #howmuch has proccsesed
        for i, f in enumerate(concurrent.futures.as_completed(futures)):
            results.append(f.result())
            if i % 200 == 0: print(f"processed {i}/{TOTAL_REQS}")

    duration = time.time() - start_time
    success_count = results.count(200)
    
    print(f"\n{OKGREEN}[RESULT]{ENDC} processed {TOTAL_REQS} in {duration:.2f}s ({TOTAL_REQS/duration:.0f} req/s)")
    if success_count > 999: success("load test passed")

    else: 
        print(f"we only succeeded to comit {success_count} of messeges")
        warn("high failure rate during load test.")


## Test 7 chaos test - maximum servers failure
def test_max_failures():
    log("\nTesting maximum servers failure (n/2 - 1)")
    
    # calculate max failures base on num of servers 
    total_servers = len(DIRECT_SERVERS)
    max_failures = (total_servers // 2) - 1
    # max_failures=9
    
    print(f"cluster size: {total_servers}")
    print(f"max failures: {max_failures}")

    # select victims 
    all_servers = [i for i in range(total_servers)]
    #followers = [s for s in all_servers if s != leader_id]
    followers=all_servers
    victims = random.sample(followers, max_failures)
    print(f"killing {len(victims)} servers: {victims}")

    # kill the victims
    for vid in victims:
        run_docker_command("stop", f"server{vid}")

    #verification
    print(f"\n{WARNING}[VERIFICATION] connectivity check:{ENDC}")
    time.sleep(2)
    for vid in victims:
        url = DIRECT_SERVERS[vid]
        try:
            requests.get(url + "/info", timeout=0.5)
            print(f"server {vid}: {FAIL}online (Error){ENDC}")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            print(f"server {vid}: {OKGREEN}offline (confirmed: port closed){ENDC}")  
    
    time.sleep(15) 

    # verify write availability
    print(f"attempting write with {max_failures} nodes down")
    
    try:
        # write request
        payload = {"type": "CREATE_AUCTION", "item_name": "maxfailureItem", "amount": 999, "user_id": CREATOR_ID}
        # Try a few times in case of election 
        success_write = False
        start_time = time.time()
        while time.time() - start_time < 30:
            try:    
                req = requests.post(f"{GATEWAY_URL}/create", json=payload, timeout=10)
                if req.status_code == 200:
                    success_write = True
                    break
                else:
                    print(f"Gateway returned {req.status_code}, retrying")  
            except:
                pass
            time.sleep(2)

        #succeded to write
        if success_write:
            success(f"system survived {max_failures} max failures, write committed.")
        else:
            fail(f"system unavailable after {max_failures} failures.")

    except Exception as err:
        fail(f"write failed during chaos: {err}")

    # recovery of the nodes
    print("resurrecting victims")
    for vid in victims:
        run_docker_command("start", f"server{vid}")
    
    print("waiting for cluster to heal")
    time.sleep(15) 





## Test 8 - request forwarding
def test_follower_forwarding():

    log("\nTesting request forwarding, from follower to leader")
    
    # find the leader
    leader_id, _ = find_leader()
    if leader_id is None: fail("no leader found")

    
    # pick a random follower
    followers = [i for i in range(len(DIRECT_SERVERS)) if i != leader_id]
    if not followers: fail("no followers found")

    
    target_follower_id = followers[0]
    #getting the target server url
    target_url = DIRECT_SERVERS[target_follower_id]
    
    print(f"leader is server {leader_id}")
    print(f"sending write request to follower server {target_follower_id} ({target_url})")


    # send write request to follower

    payload = {
        "type": "CREATE_AUCTION", 
        "item_name": "ForwardedItem", 
        "amount": 250, 
        "user_id": CREATOR_ID
    }
    
    try:
        start = time.time()

        req = requests.post(f"{target_url}/create", json=payload, timeout=5)
        duration = time.time() - start
        
        if req.status_code == 200:
            success(f"follower successfully handled request, in {duration:.3f}s")

            # verify data exists
            if "ForwardedItem" in req.text or req.json().get("item_id"):
                success("data consistency verified")

            else:
                fail("request returned 200 but data seems missing")

        else:
            fail(f"follower rejected request with {req.status_code}: {req.text}")
            
    except Exception as err:
        fail(f"forwarding test failed: {err}")






#Test 9 - Sequential consistency
def test_sequential_consistency():
    log("\nTesting sequential consistency of read")
   
    item_name = f"seqItem_{random.randint(1000,9999)}"
    
    # create Item
    requests.post(GATEWAY_URL + "/create", json={
        "type": "CREATE_AUCTION", "item_name": item_name, "amount": 10, "user_id": CREATOR_ID
    })
    
    print(f"tracking item '{item_name}' across random servers")
    
    last_seen_price = 0
    
    # perform 50 mixed Read/Write operations
    for i in range(50):
        # randomly pick Read or Write
        if random.random() < 0.3:
            # write - bid
            new_price = 10 + i + 1
            requests.post(GATEWAY_URL + "/bid", json={
                "type": "PLACE_BID", "item_id": ITEM_ID, "amount": new_price, "user_id": BIDDER_ID
            })
      
        
        else:
            # read from random servers
            target = random.choice(DIRECT_SERVERS)
            try:
                #  item_name endpoint help to find the specific test item
                req = requests.get(f"{target}/status/name?name={item_name}", timeout=1)
                if req.status_code == 200:
                    data = req.json()
                    if len(data) > 0:
                        current_price = data[0]['highest_bid']
                        
                        # CRITICAL CHECK: Time cannot move backward
                        if current_price < last_seen_price:
                            fail(f"consistency violation, server {target} returned {current_price}, but we previously saw {last_seen_price}.")
                        
                        if current_price > last_seen_price:
                            last_seen_price = current_price
            except:
                pass 
                
    success(f"passed 50 operations and saved sequential consistency. final price seen: {last_seen_price}")






## Test 10 - api prove of consept 
def test_api_spec_compliance():
    global CREATOR_ID, BIDDER_ID

    log("\nTesting API")

    #if there are auctions and registers

    if CREATOR_ID is None or BIDDER_ID is None:
        r1 = requests.post(DIRECT_SERVERS[0] + "/register").json()
        r2 = requests.post(DIRECT_SERVERS[0] + "/register").json()
        CREATOR_ID, BIDDER_ID = r1['user_id'], r2['user_id']

    

    #testing getting multiple keys
    common_name = f"Batch3DPrinter_{random.randint(1000,9999)}"
    batch_payload = [
        {"type": "CREATE_AUCTION", "item_name": common_name, "amount": 1000, "user_id": CREATOR_ID},
        {"type": "CREATE_AUCTION", "item_name": common_name, "amount": 1000, "user_id": CREATOR_ID}
    ]
    requests.post(DIRECT_SERVERS[0] + "/batch", json=batch_payload)
    time.sleep(1) # allow commit

    # test Get list 
    req = requests.get(f"{DIRECT_SERVERS[0]}/status/name?name={common_name}")
    data = req.json()
    if len(data) >= 2:
        success("API verified: getting value of multiple keys supported.")
    else:
        fail(f"API failed: expected list of items, got {data}")

    # getting the value of a key
    target_item = data[0]['item_id']
    req = requests.get(f"{DIRECT_SERVERS[0]}/status/item_id?item_id={target_item}")
    if req.status_code == 200 and req.json()['item_id'] == target_item:
        success("API verified: getting value of a single key supported")

    else:
        fail("API failed: get single key")



    # using PLACE_BID for updating a key 
    bid_res = requests.post(DIRECT_SERVERS[0] + "/bid", json={
        "type": "PLACE_BID", "item_id": target_item, "amount": 1500, "user_id": BIDDER_ID
    })
    if bid_res.status_code == 200:
        success("API Verified: updating a key, linearizable write supported")
    else:
        fail("API Failed: Update key")
    
    #creating new clients
    r1 = requests.post(DIRECT_SERVERS[0] + "/register").json()
    r2 = requests.post(DIRECT_SERVERS[0] + "/register").json()
    bidder1, bidder2 = r1['user_id'], r2['user_id']

    # updating a list of keys
    update_batch = [
        {"type": "PLACE_BID", "item_id": data[0]['item_id'], "amount": 2000, "user_id": bidder1},
        {"type": "PLACE_BID", "item_id": data[1]['item_id'], "amount": 2000, "user_id": bidder2}
    ]
    req = requests.post(DIRECT_SERVERS[0] + "/batch", json=update_batch)
    if req.status_code == 200:
        success("API verified: updating a list of keys (batch write) supported")
    else:
        print(f"{FAIL} Batch update failed: {req.text}{ENDC}")
        fail("API failed: batch update")


    # read modify write using PLACE_BID
    low_bid = requests.post(DIRECT_SERVERS[0] + "/bid", json={
        "type": "PLACE_BID", "item_id": target_item, "amount": 5, "user_id": BIDDER_ID
    })
    

    if "Rejected" in low_bid.text or low_bid.status_code != 200:
        success("API verified: read modify write - atomic logic check supported.")
    else:
        fail(f"API failed: RMW logic check failed. Response: {low_bid.text}")






if __name__ == "__main__":
    print("Starting Comprehensive Distributed Systems Test\n")
    try:
        # wait for servers to be ready before starting
        recover_all_nodes()
        wait_for_cluster() 
        # #test 1
        test_basic_flow() 
        #test 2  
        test_batching()     
        #test 3
        test_consistency()
        #test 4
        test_leader_failure()
        #test 5
        test_follower_recovery()
        #test 6
        test_load()
        #test 7
        test_max_failures()    
        time.sleep(10)
        #test 8
        test_follower_forwarding() 
        #test 9
        test_sequential_consistency() 
        #test 10
        test_api_spec_compliance()    
        
        print("\n[INFO] Cooling down for load test...")
        time.sleep(15) 
        test_load()
        
    except KeyboardInterrupt:
        print("\nTest Cancelled.")
    except Exception as e:
        print(f"\n{FAIL}[ERROR]{ENDC} Test suite failed: {e}")
    finally:
        recover_all_nodes()
