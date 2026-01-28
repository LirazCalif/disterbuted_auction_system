export type CommandType =
  | 'REGISTER'
  | 'CREATE_AUCTION'
  | 'PLACE_BID'
  | 'CLOSE_AUCTION'
  | 'DELETE_AUCTION';

export interface Command {
  type: CommandType;
  item_id?: number;
  item_name?: string;
  user_id?: number;
  amount?: number;
  timestamp: number;
}