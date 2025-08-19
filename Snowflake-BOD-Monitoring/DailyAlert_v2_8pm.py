import snowflake.connector
import pandas as pd
from datetime import timedelta 
from datetime import datetime as dat
import datetime
import os
import subprocess
# import schedule
import time
from threading import Thread

def runReport1():
    config = {}
    outconf = {}
    with open("config.txt") as f:
        for line in f:
            if "=" in line:
                a = line.split("=")
                config[a[0]] = a[1]

    outconf["sched"] = config["schedB"].strip()
    outconf["lag"] = config["lagB"].strip()
    outconf["env"] = config["env"].strip()
    outconf["user"] = config["user"].strip()
    outconf["password"] = config["password"].strip()
    outconf["account"] = config["account"].strip()
    outconf["warehouse"] = config["warehouse"].strip()
    outconf["role"] = config["role"].strip()
    
    outconf["To"] = config["ToB"].strip()
    outconf["From"] = config["FromB"].strip()
    outconf["CC"] = config["CCB"].strip()
    outconf["Subject"] = config["SubjectB"].strip()
    
    outconf["start"] = config["schedB"].strip()
    outconf["end"] = config["schedB"].strip()
    outconf["scale"] = config["scaleB"].strip()
    
    outconf["typ"] = 1
    
    runReport(outconf)
def runReport2():
    config = {}
    outconf = {}
    with open("confecig.txt") as f:
        for line in f:
            if "=" in line:
                a = line.split("=")
                config[a[0]] = a[1]

    outconf["sched"] = config["schedC"].strip()
    outconf["lag"] = config["lagC"].strip()
    outconf["env"] = config["env"].strip()
    outconf["user"] = config["user"].strip()
    outconf["password"] = config["password"].strip()
    outconf["account"] = config["account"].strip()
    outconf["warehouse"] = config["warehouse"].strip()
    outconf["role"] = config["role"].strip()
    
    outconf["To"] = config["ToC"].strip()
    outconf["From"] = config["FromC"].strip()
    outconf["CC"] = config["CCC"].strip()
    outconf["Subject"] = config["SubjectC"].strip()
    
    outconf["start"] = config["schedB"].strip()
    outconf["end"] = config["schedC"].strip()
    outconf["scale"] = config["scaleC"].strip()
    
    outconf["typ"] = 2
    
    runReport(outconf)

def runReport(config):


    sched = config["sched"]
    lag = config["lag"]
    env = config["env"]
    user = config["user"]
    password = config["password"]
    account = config["account"]
    warehouse = config["warehouse"]
    role = config["role"]
    To = config["To"]
    From = config["From"]
    CC = config["CC"]
    Subject = config["Subject"]
    Start = config["start"]
    End = config["end"]
    Typ = config["typ"]
    scale = config["scale"]
    
    sch = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    role=role,
    database='EDM_REFINED_' + env,
    schema='DW_R_LOCATION'
    ) 

    tbl = " SELECT * FROM EDM_REFINED_" + env + ".SCRATCH.EDM_MONITORING_BODDATA_APPID;"
    filename='EXCEPTIONS_' + datetime.date.today().strftime("%m%d") + '.xlsx'
    filecreated = False
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    workbook=writer.book
    def getEcatstat(bodpd,n):
        """  returns ecatalog status  """       
        
        cnt = len(bodpd.index)
        if cnt==1 and bodpd["CNT"].head(1).item() == 0:
            return "No exceptions observed"
        else:            
            query = ("CALL EDM_REFINED_" + env + ".SCRATCH.GET_MONITORING_BODDATA_APPID('EDM_CONFIRMED_OUT_" + env + "','" + lag + "','" + n + "',true,false,false,true); ")
             
            sch.cursor().execute(query)
             
            query = (tbl)
            if len(n)>31:
                n=n[0:31]
                            
            df = pd.read_sql_query(query ,sch)

            del bodpd["TABLENAME"]
            del bodpd["KAFKAOUT"]
            del bodpd["KAFKAOUTQUEUE"]
            
            if len(df.index)>0:
                worksheet=workbook.add_worksheet(n)
                writer.sheets[n] = worksheet
                worksheet.write_string(0, 0,"")
                bodpd.to_excel(writer,sheet_name=n,startrow=1 , startcol=0, index=False)
                worksheet.write_string(bodpd.shape[0] + 4, 0, "")
                df.to_excel(writer,sheet_name=n,startrow=bodpd.shape[0] + 5, startcol=0, index=False)
                
            
            nonlocal filecreated
            filecreated=True
            return str(bodpd['CNT'].sum()) + " Exceptions observed (see attached report)"
        
    def getBodstatus(z,df):
        """  returns bod status  """        
        c = len(df.index)
        if c==0:
            return "No data loaded"
        status = ""
        for i in df.itertuples():
            status=i.STATUS
        if z=="LOAD":
            if status == "LOADED":
                return "Success"
            else:
                return "Failure"
        else:
            if status == "SKIPPED":
                return "No data loaded"
            elif status == "FAILED" or status == "CANCELLED":
                return "Failure"
            else:
                return "Success"            
    
    
    # Create dataframes for the bods' monitor query resultset
           

    query = ("SELECT TASKNAME, ZONE, SCHEMANAME,NTILE(" + str(scale) + ") OVER(ORDER BY TASKNAME) AS G FROM EDM_REFINED_" + env + ".SCRATCH.APPDTASKS WHERE "
             "EffectiveEndDate IS NULL")
   
    bod_df = pd.read_sql_query(query ,sch);
    
    
    query = ("""SELECT A.TASKNAME AS LOAD,IFNULL(B.TASKNAME,'') AS REF,IFNULL(C.TASKNAME,'') AS CONF,A.CI
FROM (SELECT TASKNAME,CI FROM EDM_REFINED_PRD.SCRATCH.APPDTASKS WHERE EffectiveEndDate IS NULL AND ZONE ='LOAD') A
LEFT JOIN (SELECT TASKNAME,CI FROM EDM_REFINED_PRD.SCRATCH.APPDTASKS WHERE EffectiveEndDate IS NULL AND ZONE ='REFINED') B ON A.CI=B.CI
LEFT JOIN (SELECT TASKNAME,CI FROM EDM_REFINED_PRD.SCRATCH.APPDTASKS WHERE EffectiveEndDate IS NULL AND ZONE ='CONFIRMED') C ON A.CI=C.CI""")
   
    mainbods_df = pd.read_sql_query(query ,sch);
        
        
    #sch.cursor().execute(query)
    #query = (tbl)
    
    #bod_df = pd.read_sql_query(query ,sch)
    
    query = ("CALL EDM_REFINED_" + env + ".SCRATCH.GET_MONITORING_BODDATA_APPID('EDM_REFINED_" + env + "','" + lag + "','',false,true,true,false); ")
    sch.cursor().execute(query)         
    query= (tbl)
    
    lastconfupd = pd.read_sql_query(query ,sch);
    
    query = ("CALL EDM_REFINED_" + env + ".SCRATCH.GET_MONITORING_BODDATA_APPID('EDM_REFINED_" + env + "','" + lag + "','',false,true,false,false); ")
    sch.cursor().execute(query)
    query = (tbl)
    
    lastoutupd = pd.read_sql_query(query ,sch);
    
    query = ("CALL EDM_REFINED_" + env + ".SCRATCH.GET_MONITORING_BODDATA_APPID('EDM_CONFIRMED_OUT_" + env + "','" + lag + "','',true,false,false,false); ")
    sch.cursor().execute(query)
    query = (tbl)
    
    kafkaoutexc_df = pd.read_sql_query(query ,sch)
    
    loadz = {}
    refz = {}
    confz = {}
    koutz = {}
    kqueuez = {}
    excz = {}    
    listexc = set()
    alltasks = set()
    koexc = {}
    kqexc = {}
    allnonbodtasks = set()
    
    listexcviews = set()
    
    bodwarning = set()
    excwarning = set()
    kqwarning = set()
    failalert = {}
    lastbodupd = {}
    lastexcupd = {}
    lastkqexcupd = {}
    noexc = set()
    
    for i in lastconfupd.itertuples():
        lastbodupd[i.BOD] = i.DTIME + "," + i.FREQ
        if i.WARNING==1 and i.BOD!="RETAILSTORE":
            bodwarning.add(i.BOD)
    
    for i in lastoutupd.itertuples():
        lastexcupd[i.TBL] = i.DTIME
        if i.WARNING==1 and i.TBL!="ECATALOG_RETAIL_STORE_PRICE_AREA_EXCEPTIONS":
            excwarning.add(i.TBL)
        lastkqexcupd[i.TBL] = i.KQDTIME
        if i.KQWARNING==1 and i.TBL!="ECATALOG_RETAIL_STORE_PRICE_AREA_EXCEPTIONS":
            kqwarning.add(i.TBL)
    
    for i in kafkaoutexc_df.itertuples(): 
        if i.TABLENAME!="":            
            if i.KAFKAOUT=="":  
                listexcviews.add(i.TABLENAME)
            else:
                listexc.add(i.TABLENAME)
                if i.MSG=="N/A":
                    noexc.add(i.TABLENAME)
            koexc[i.TABLENAME] = i.KAFKAOUT
            kqexc[i.TABLENAME] = i.KAFKAOUTQUEUE
        
            
        
    for i in listexc:
        if i not in noexc:
            f = kafkaoutexc_df[kafkaoutexc_df["TABLENAME"] == i]
            excz[i] = getEcatstat(f,i) 
        
    for i in listexcviews:
        f = kafkaoutexc_df[kafkaoutexc_df["TABLENAME"] == i]
        excz[i] = getEcatstat(f,i) 

    writer.save()
    
    def procthread(d):
        for i in d.itertuples():
            
            if i.ZONE=="LOAD":
                query = ("SELECT * FROM TABLE(EDM_REFINED_" + env + ".SCRATCH.ALERT_LOADDATA('" + i.ZONE + "','EDM_REFINED_" + env + "." + i.SCHEMANAME + "." + i.TASKNAME  + "','-" + lag + "'))")
                dd = pd.read_sql_query(query ,sch)
                loadz[i.TASKNAME] = getBodstatus("LOAD",dd)
                alltasks.add(i.TASKNAME)
                if loadz[i.TASKNAME] == "Failure":
                    failalert[i.TASKNAME] = "LOAD"
            elif i.ZONE=="REFINED":
                r = mainbods_df.loc[mainbods_df["REF"]==i.TASKNAME,'LOAD'].iloc[0]
                query = ("SELECT * FROM TABLE(EDM_REFINED_" + env + ".SCRATCH.m('" + i.ZONE + "','" + i.TASKNAME + "','-" + lag + "','EDM_REFINED_" + env + "'))")
                dd = pd.read_sql_query(query ,sch)
                refz[i.TASKNAME] = getBodstatus("REFINED",dd)
                if refz[i.TASKNAME] == "Failure":
                    failalert[r] = "REFINED"
            elif i.ZONE=="CONFIRMED":
                c = mainbods_df.loc[mainbods_df["CONF"]==i.TASKNAME,'LOAD'].iloc[0]
                query = ("SELECT * FROM TABLE(EDM_REFINED_" + env + ".SCRATCH.ALERT_TASKDATA('" + i.ZONE + "','" + i.TASKNAME + "','-" + lag + "','EDM_CONFIRMED_" + env + "'))")
                dd = pd.read_sql_query(query ,sch)
                confz[i.TASKNAME] = getBodstatus("CONFIRMED",dd) 
                if confz[i.TASKNAME] == "Failure":
                    failalert[c] = "CONFIRMED"
            elif i.ZONE=="KAFKAOUTQUEUE":
                query = ("SELECT * FROM TABLE(EDM_REFINED_" + env + ".SCRATCH.ALERT_TASKDATA('" + i.ZONE + "','" + i.TASKNAME + "','-" + lag + "','EDM_CONFIRMED_OUT_" + env + "'))")
                dd = pd.read_sql_query(query ,sch)
                kqueuez[i.TASKNAME] = getBodstatus("KAFKAOUTQUEUE",dd)
                allnonbodtasks.add(i.TASKNAME)
                if kqueuez[i.TASKNAME] == "Failure":
                    failalert[i.TASKNAME] = "KAFKAOUTQUEUE"
            elif i.ZONE=="KAFKAOUT":
                query = ("SELECT * FROM TABLE(EDM_REFINED_" + env + ".SCRATCH.ALERT_TASKDATA('" + i.ZONE + "','" + i.TASKNAME + "','-" + lag + "','EDM_CONFIRMED_OUT_" + env + "')) UNION ALL SELECT * FROM TABLE(EDM_REFINED_" + env + ".SCRATCH.ALERT_TASKDATA('" + i.ZONE + "','" + i.TASKNAME + "','-" + lag + "','EDM_ANALYTICS_" + env + "'))")
                dd = pd.read_sql_query(query ,sch)
                koutz[i.TASKNAME] = getBodstatus("KAFKAOUT",dd)
                allnonbodtasks.add(i.TASKNAME)
                if koutz[i.TASKNAME] == "Failure":
                    failalert[i.TASKNAME] = "KAFKAOUT"
    

    threads = []
    n = int(scale)
    while n>0:
        t = Thread(target=procthread, args=(bod_df.loc[bod_df["G"] == n],))
        threads.append(t)
        n-=1

    for x in threads:
        x.start()
    
    for x in threads:
        x.join()
        
    s = dat.strptime(Start,"%H:%M")
    e = dat.strptime(End,"%H:%M")
    fromdate = ""
    if Typ==1:
        fromdate = (datetime.date.today() - timedelta(days=1)).strftime("%m/%d/%Y") +  " " + s.strftime("%I:%M %p")
    else:
        fromdate = (datetime.date.today()).strftime("%m/%d/%Y") +  " " + s.strftime("%I:%M %p")
    todate = datetime.date.today().strftime("%m/%d/%Y") +  " " + e.strftime("%I:%M %p")
        
    htmlhead = ("<p>Hi All</p><br />"
                "<p style='margin-bottom: 15px;'>Please find the monitoring update for " + fromdate + " to " + todate + " below.</p>")
    html = (                
            "<table style='margin-bottom: 5px; font-family: arial, sans-serif; border-collapse: collapse; width: 100%'>"
            "<tr style='background-color: #dddddd;'>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>Bod Name</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>Load</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>Refined</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>Confirmed</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>Confirmed Last Loaded</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>Frequency</th>"
            "</tr>")
    
    for i in alltasks:
        b = i.replace("ESED_","")
        if i in failalert or i in bodwarning:
            if i in failalert:
                htmlhead+="<i style='display: block;'>DANGER! Job failed for BOD " + b + " in ZONE " + failalert[i] + ".</i>"
                html+="<tr style='background-color: #FF6666;'>"
      
            if i in bodwarning and i not in failalert:
                htmlhead+="<i style='display: block;'>WARNING! For the past two days no data loaded into confirmed zone for high volume bod: " + b + "</i>"
                html+="<tr style='background-color: yellow;'>"
        else:
            html+="<tr>"  
        
        rk = mainbods_df.loc[mainbods_df["LOAD"]==i,'REF'].iloc[0]
        r = "N/A" if rk=="" else refz[rk]

        ck = mainbods_df.loc[mainbods_df["LOAD"]==i,'CONF'].iloc[0]
        c = "N/A" if ck=="" else confz[ck]
        
        
        html+=(            
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + (b[0:1]).upper() + b[1:len(b)].lower() + "</td>"
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + loadz[i] + "</td>"
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + r + "</td>"
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + c + "</td>")

        if i in lastbodupd:
            l = lastbodupd[i].split(",")       
            html+="<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + l[0] + "</td>"
            html+="<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + l[1] + "</td></tr>"
        else:
            html+="<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>...........</td>"
            html+="<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>...........</td></tr>"
    
    html+="</table>"
    html+=("<table style='font-family: arial, sans-serif; border-collapse: collapse; width: 100%'>"
            "<tr style='background-color: #dddddd;'>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>Task Name</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>Exception</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>KafkaOutQueue</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>KafkaOut</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>KOut Last Loaded</th>"
            "<th style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>KQueue Last Published</th>"
            "</tr>")
    for i in listexc:
        if (kqexc[i] in failalert or koexc[i] in failalert) or (i in excwarning or i in kqwarning):
            if kqexc[i] in failalert or koexc[i] in failalert:
                k =  koexc[i] if kqexc[i] not in failalert else kqexc[i]
                htmlhead+="<i style='display: block;'>DANGER! Job failed for task " + k + " in ZONE " + failalert[k] + " for out table " + i.replace("_EXCEPTIONS","") + ".</i>"
                html+="<tr style='background-color: #FF6666;'>"

            if (i in excwarning or i in kqwarning) and kqexc[i] not in failalert and koexc[i] not in failalert: 
                y=""            
                if i in excwarning:
                    htmlhead+="<i style='display: block;'>WARNING! For the past two days no data loaded into KafkaOut table: " + i.replace("_EXCEPTIONS","") + "</i>"
                    y = "<tr style='background-color: yellow;'>"
                    
                if i in kqwarning:
                    htmlhead+="<i style='display: block;'>Danger! For over two days no messages were published in KafkaOutQueue topic: EDDW_C02_ECAT_" + i.replace("_EXCEPTIONS","").replace("ECATALOG_","").replace("ROGPRICE","PRICE").replace("STOREPRICE","PRICE") + "</i>"
                    y = "<tr style='background-color: #FF6666;'>"
                html+=y   
                
        else:
            html+="<tr>"
        noe = "N/A" if i in noexc else excz[i]             
        html+=(
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + (i[0:1]).upper() + i[1:len(i)].lower().replace("_exceptions","") + "</td>"
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + noe + "</td>"               
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + kqueuez[kqexc[i]] + "</td>"
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + koutz[koexc[i]] + "</td>"    
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + lastexcupd[i] + "</td>"                
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + lastkqexcupd[i] + "</td></tr>")
                
   
                
        
    for i in listexcviews:
        html+=("<tr>"
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + (i[0:1]).upper() + i[1:len(i)].lower() + "</td>"
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + excz[i] + "</td>"               
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>N/A</td>"
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>N/A</td>" 
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>...........</td>"
                "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>...........</td>"
                "</tr>"
                )

        
    for i in allnonbodtasks:   
        if i not in kqexc.values() and i not in koexc.values(): 
            if i in failalert:
                htmlhead+="<i style='display: block;'>DANGER! Job failed for Task " + i + " in ZONE " + failalert[i] + ".</i>"
                html+="<tr style='background-color: #FF6666;'>"
            else:
                html+="<tr>"
            html+=(
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + (i[0:1]).upper() + i[1:len(i)].lower() + "</td>"
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>N/A</td>"
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>N/A</td>"
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>" + koutz[i] + "</td>"
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>...........</td>"
            "<td style='border: 1px solid #dddddd;text-align: left;padding: 8px;'>...........</td>"
            "</tr>")        
            
    html+=("</table><br />"
            "Thanks.")
    html = htmlhead + html
    
    if filecreated:
        command = "openssl base64 < /var/tmp/edm_dataops/monitoring/" + filename
        res = subprocess.check_output(command,universal_newlines=True,shell=True)  

        str2 = '''From: {}
To: {}
CC: {}
Subject: {}
Mime-Version: 1.0
Content-Type: multipart/mixed; boundary="19032019ABCDE"

--19032019ABCDE
Content-Type: text/html; charset="US-ASCII"
Content-Transfer-Encoding: 7bit
Content-Disposition: inline

{}

--19032019ABCDE
Content-Type: application;
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="{}"

{}

--19032019ABCDE--'''.format(From,To,CC,Subject,html,filename,res)
  
        outfile = "emd.eml"
        with open(outfile,mode = "w", encoding = 'utf-8') as f:
            f.write(str2) 
        html_output=('sendmail -t < emd.eml')
        subprocess.call(html_output,shell=True )
        os.remove("/var/tmp/edm_dataops/monitoring/" + filename)
    else:
        htmlfile = "edmalert.html"
        with open(htmlfile,mode = "w", encoding = 'utf-8') as f:
            f.write(html) 
        html_output=('outputFile=edmalert.html \n'
           '( \n ' 
           'echo  "From: ' + From + ' " \n'
          'echo "To:  ' + To + ' " \n'
          'echo "MIME-Version: 1.0" \n'
          'echo "Subject: ' + Subject + '" \n'
          'echo "Content-Type: text/html" \n'
          'cat $outputFile \n'
          ' \n) | sendmail -t ')

        subprocess.call(html_output,shell=True )

runReport2()
# schedule.every().day.at("09:00").do(runReport1).tag('dailyalert1')
# schedule.every().day.at("18:00").do(runReport2).tag('dailyalert2')

# while 1:
#     schedule.run_pending()
#     time.sleep(1)