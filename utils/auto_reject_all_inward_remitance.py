from utils.query_db_data import get_all_inward_processing_order
from utils.create_transaction_MT103 import inward_result_update

def auto_accep_all_inward_remitance(db_type):
    inward_processing_order = get_all_inward_processing_order(db_type)
    for order in inward_processing_order:
        inward_bank_req_id = order['bank_req_id']
        respone = inward_result_update(inward_bank_req_id,1,"high risk",db_type)
        print(f'流水号：{inward_bank_req_id}，处理结果：{respone}')
    print("批量拒绝跨行汇入 - - 完成")
        
if __name__=="__main__":
    print(auto_accep_all_inward_remitance("receive_bank"))