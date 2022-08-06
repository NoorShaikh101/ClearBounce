import os
import sys
import csv
import logging
from datetime import datetime
#from com.ziclix.python.sql import zxJDBC
#import com.mysql.cj.jdbc.Driver
import shutil
import mysql.connector


#file_path = sys.argv[1]
#Log_Path = sys.argv[2]

current_dt = datetime.strftime(datetime.now(), '%y%m%d')
#logging.basicConfig(filename=str(Log_Path) + "ClearBounce_" + current_dt + ".log", format='%(asctime)s %(message)s', filemode='a')
logger = logging.getLogger()

try:
    # // Database Connection //
    logger.setLevel(logging.INFO)
    url = "jdbc:mysql://192.168.0.200:3306/UAT_7?autoReconnect=true&useSSL=false"
    user = 'root'
    password = 'password'
    conn = zxJDBC.connect(url, user, password, "com.mysql.cj.jdbc.Driver")
    cursor = conn.cursor()

    query1 = "select rrt.BANK_ID, rrt.MODULE_ID, rrt.BUSINESS_DT, rrt.Recon_batch_no, rrt.recon_no, rrt.recon_seq_no, rrt.leg2_ref1, rrt.leg2_Ref2, rrt.leg2_Ref3, rrt.LEG_2_CR_AMT, rrt.LEG_2_DR_AMT, rrt.LEG_2_NET_AMT, rrt.PROCESS_DT, rrt.ADD_INFO9, rrt.ADD_INFO10, rrt.LEG_1_ID, rrt.LEG_2_ID, rrt.TERMINAL_ID, rlt.TEXT_1, rrt.RECON_MODE FROM RC_RECONCILED_TXN as rrt join RC_LEG_TXN AS rlt on rrt.recon_Seq_No = rlt.recon_Seq_No WHERE rrt.leg_2_Id != 0 and rrt.module_Id = 115 and rrt.recon_Status = 'S' and rrt.recon_Mode != 'M' and ( rrt.clb_status IS NULL OR rrt.clb_status != 'Y' ) and rlt.text_1 not like '%l_eleTxnArray%' and rrt.terminal_Id = '110181000000'"

    query2 = "select rrt.bank_Id, rrt.module_Id, rrt.business_Dt, rrt.recon_Seq_No, rrt.recon_Batch_No, rrt.recon_No, rrt.leg1_Ref1, rrt.leg1_Ref2, rrt.leg1_Ref3, rrt.leg_1_Cr_Amt, rrt.leg_1_Dr_Amt, rrt.leg_1_Net_Amt, rrt.process_Dt, rrt.ADD_INFO9, rrt.leg_1_Id, rrt.leg_2_Id, rrt.ADD_INFO10, rlt.num_1, rrt.terminal_Id, rrt.add_Info3, rlt.text_3, rlt.long_Text_3 FROM RC_RECONCILED_TXN as rrt, RC_LEG_TXN AS rlt WHERE rrt.leg_1_Id !=0 AND rrt.recon_Seq_No = rlt.recon_Seq_No and rrt.leg_1_Dr_Amt != 0 AND rrt.module_Id =115 AND rrt.recon_Status='S' AND rrt.recon_Mode != 'M' AND (clb_status IS NULL OR clb_status != 'Y') and rrt.terminal_Id='110181000000'"

    query3 = "select rrt.bank_Id, rrt.module_Id, rrt.business_Dt, rrt.recon_Seq_No, rrt.recon_Batch_No, rrt.recon_No, rrt.leg1_Ref1, rrt.leg1_Ref2, rrt.leg1_Ref3, rrt.leg_1_Cr_Amt, rrt.leg_1_Dr_Amt, rrt.leg_1_Net_Amt, rrt.process_Dt, rrt.add_Info9, rrt.leg_1_Id, rrt.leg_2_Id, rrt.add_Info10, rlt.num_1, rrt.terminal_Id, rrt.add_Info3, rlt.text_3, rlt.long_Text_3 FROM RC_RECONCILED_TXN as rrt, RC_LEG_TXN AS rlt WHERE rrt.leg_1_Id !=0 AND rrt.recon_Seq_No = rlt.recon_Seq_No and rrt.leg_1_Dr_Amt != 0 AND rrt.module_Id =115 AND rrt.recon_Status != 'S' AND (clb_status IS NULL OR clb_status != 'Y') and rrt.terminal_Id='110181000000'"

    query4 = "SELECT CONCAT(LEG2_REF1,LEG_2_NET_AMT,TERMINAL_ID), count(LEG2_REF1) as DUP_KEY FROM RC_RECONCILED_TXN WHERE MODULE_ID=115 AND LEG_1_ID = 0 AND LEG_2_CR_AMT != 0 AND TERMINAL_ID='110181000000' AND (CLB_STATUS IS NULL OR CLB_STATUS != 'Y') GROUP BY CONCAT(LEG2_REF1, LEG_2_NET_AMT, TERMINAL_ID) HAVING DUP_KEY > 1 ORDER BY LEG2_REF1, LEG_2_NET_AMT, TERMINAL_ID"

    query5 = "select concat(leg2_Ref1, leg_2_Dr_Amt, terminal_Id), count(leg2_Ref1) from RC_RECONCILED_TXN where module_Id =115 and leg_1_Id = 0 and leg_2_Dr_Amt !=0 and terminal_Id='110181000000' and (clb_status IS NULL OR clb_status != 'Y') and length(leg2_Ref1) <=10 group by concat(leg2_Ref1, leg_2_Dr_Amt, terminal_Id) order by leg2_Ref1, leg_2_Dr_Amt, terminal_Id;"

    cursor.execute(query1)
    temp1 = cursor.fetchall()
    if (temp1):
        for row in temp1:
            data_dict1 = {str(row[6]) + str(row[10]) + str(row[17]): str(row[0]) + "," + str(row[1])+
                          "," + str(row[2]) + "," + str(row[3]) + "," + str(row[4]) + "," + str(row[5]) + "," + str(
                              row[6]) + "," + str(row[7]) + "," + str(row[8]) + "," + str(row[9]) + "," + str(
                              row[10]) + "," + str(row[11]) + "," + str(row[12]) + "," + str(row[13]) + "," + str(
                              row[14]) + "," + str(row[15]) + "," + str(row[16]) + "," + str(row[17]) + "," + str(
                              row[18]) + "," + str(row[19])}
            data_dict1.update(data_dict1)

        for key in sorted(data_dict1.iterkeys()):
            print
            "%s: %s" % (key, data_dict1[key])

    '''
    ref1 = []
    cursor.execute(query1)
    temp1 = cursor.fetchall()
    if(temp1):
        for row in temp1:
            ref1.append(str(row[7])
        ref1New = [x.lstrip('0') for x in ref1]

    '''
    '''
    records =  str(row[0])+","+str(row[1])+","+str(row[2])+","+str(row[3])+","+str(row[4])+","+str(row[5])+","+str(row[6])+","+str(row[7])+","+str(row[8])+","+str(row[9])+","+str(row[10])+","+str(row[11])+","+str(row[12])+","+str(row[13])+","+str(row[14])+","+str(row[15])+","+str(row[16])+","+str(row[17])+","+str(row[18])+","+str(row[19])+","+str(row[20])

    records = cursor.execute(query1)
    records = cursor.fetchall()
    logger.info("query executed  Successfully.")
    file = open(file_path + "ONELMS_JV_Handoff"+current_dt+".csv", "w")
    file.write("BANK_ID, BATCH_ID, MODULE_ID, BUSINESS_DT, Recon_batch_no, recon_no, recon_seq_no, leg2_ref1, leg2_Ref2, leg2_Ref3,LEG_2_CR_AMT,LEG_2_DR_AMT,LEG_2_NET_AMT,PROCESS_DT,ADD_INFO9,ADD_INFO10,LEG_1_ID,LEG_2_ID,TERMINAL_ID,TEXT_1,RECON_MODE")
    file.write('\n')
    rec =0;
    for row in records:
        record1 = str(row[0])+","+str(row[1])+","+str(row[2])+","+str(row[3])+","+str(row[4])+","+str(row[5])+","+str(row[6])+","+str(row[7])+","+str(row[8])+","+str(row[9])
        file.write(record1)
        file.write('\n')
        rec =1
    if(rec == 0):
        logger.info("No Record Found for Processing.")
    else :
        logger.info("Script executed Sucessfully.")
        file.close()
    def queryExecution():
        update_query = "Update RC_JV_M2M_TXN set STATUS='F' where STATUS = 'A' and SOURCE_SYSTEM='ONELMS-SYSTEM'"
        cursor.execute(update_query)
        conn.commit()
    queryExecution()
    '''
    conn.close()
    print("FANTAILP_SUCCESS")
except Exception as err:
    logger.setLevel(logging.ERROR)
    logger.error(err)
