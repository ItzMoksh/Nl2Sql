
# coding: utf-8

# In[1]:


from nltk.corpus import wordnet as wn
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize
import difflib
import MySQLdb
import demjson
from dateutil.parser import parse
StartDate = ""
EndDate = ""
import re
from nltk.stem import WordNetLemmatizer

con = MySQLdb.connect("ip","user","password","Database_Name")
cur = con.cursor()


# In[4]:



cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='analytics_temp'")
table_list = []
inv_cols = ['cart_id','total_offer','taxes','currency','offer','amount_with_offer','taxes','shipping_charges','gift_packing_charges','total_payable_amount','is_gift']
for i,row in enumerate(cur):
    tmp = (str(row).replace("'",""))
    tmp = tmp.replace("(","")
    tmp = tmp.replace(",","")
    tmp = tmp.replace(")","")
    table_list.append(tmp)
#print (table_list)



fp = open('C:/Users/ItzMokshBiches/Desktop/MoglixProject/ln2sql-master/ln2sql/stopwords/english.txt',encoding='utf-8')
sw = fp.read()
CheckLike = 0
Sem_dict = {}
pot_tables = []
QE = 0
DateCol = ["updated_date","created_date","updated_at","created_at"]
detail_list = ['info','detail']
order_list = ["id"]
id_list = ['product','order','session','user','agent','cart']
amount_list = ['total','offer','taxes','payable']
product_list = ['name','key','features','quantity','unit','price','img']
Lemma = WordNetLemmatizer()

##
def get_cols(tablename):
    sql = "select * from {} limit 1".format(tablename)
    cur.execute(sql)
    return [x[0] for x in cur.description]
##


##
def CheckDate(word):
    StartDate = ""
    EndDate = ""
    Str = ""
    try:
        Str = wn.synsets(word)[0]
    except:
        Str = None
    if Str is not None:
        if Str.wup_similarity(wn.synsets("Month")[0]) == 0.8571428571428571:
            tmp = str(parse(word))
            tmp = list(tmp)
            tmp[8] = '0'
            tmp [9] = '1'
            StartDate = "".join(tmp)
            print (StartDate)
            tmp [8] = '3'
            tmp [9] = '1'
            tmp = ("".join(tmp)).replace("00:00:00","23:59:59")
            EndDate = "".join(tmp)
            print (EndDate)
            Sem_dict[word]['date'] = 1
            return str(StartDate),str(EndDate)
    try:
        Date = parse(word)
        if (re.match(r'\w+\d*',word)) is not None:
            tmp = str(parse(word))
            tmp = list(tmp)
            tmp[8] = '0'
            tmp [9] = '1'
            StartDate = "".join(tmp)
            print (StartDate)
            tmp [8] = '3'
            tmp [9] = '1'
            tmp = ("".join(tmp)).replace("00:00:00","23:59:59")
            EndDate = "".join(tmp)
            print (EndDate)
            Sem_dict[word]['date'] = 1
            return str(StartDate),str(EndDate)
        if len(word) == 6 or len(word) == 7:
            tmp = list(str(Date))
            tmp[8] = '0'
            tmp [9] = '1'
            StartDate = "".join(tmp)
            tmp [8] = '3'
            tmp [9] = '1'
            tmp = ("".join(tmp)).replace("00:00:00","23:59:59")
            EndDate = "".join(tmp)
            Sem_dict[word]['date'] = 1
            return str(StartDate),str(EndDate)
        elif len(word) == 4:
            tmp = list(str(Date))
            tmp[5] = '0'
            tmp[6] = '1'
            tmp[8] = '0'
            tmp [9] = '1'
            StartDate = "".join(tmp)
            tmp[5] = '1'
            tmp[6] = '2'
            tmp [8] = '3'
            tmp [9] = '1'
            tmp = ("".join(tmp)).replace("00:00:00","23:59:59")
            EndDate = "".join(tmp)
            Sem_dict[word]['date'] = 1
            return str(StartDate),str(EndDate)
        elif len(word) == 10:
            StartDate = str(parse(word))
            EndDate = StartDate.replace("00:00:00","23:59:59")
            Sem_dict[word]['date'] = 1
            return str(StartDate),str(EndDate)
        else:
            return None
    except:
        return None
##

    
##
def remove_duplicates(values):
    output = []
    seen = set()
    for value in values:
        if value not in seen:
            output.append(value)
            seen.add(value)
    return output
##


##
def CheckWhere(nl):
    like_phrase = ""
    found = 0
    for i,key in enumerate(Sem_dict):
        if Sem_dict[key]['pot_cols'] == [] and Sem_dict[key]['pot_tables'] == []:
            Sem_dict[key]['where'] = 1
        elif str(key) == "orders" or str(key) == "order":
            found  = 1
            continue
        else:
            found = 1
            continue
        #if not (Sem_dict[key]['pot_tables'] == []):
        found = 0
        for table in table_list:
            if found == 1:
                break
            sql = "select * from {} limit 1".format(table)
            cur.execute(sql)
            cols = [x[0] for x in cur.description]
            opt = ""
            GREAT = ["greater","gt",">","larger","more than"]
            SMALL = ["smaller","st","<","lesser than","less than"]
            for kw in GREAT:
                if kw in nl:
                    opt = ">"
            if opt == "":
                for kw in SMALL:
                    if kw in nl:
                        opt = "<"
            if opt == "":
                opt = "="
            close_col = []
            try:
                close_col.append(Sem_dict[Clean[Clean.index(key)-1]]['pot_cols'])
            except:
                DoNothing = "DoNootjng"
            try:
                close_col.append(Sem_dict[Clean[Clean.index(key)+1]]['pot_cols'])
            except:
                okay = "okay"
            for col in close_col:
                if col in DateCol:
                    continue
                if col not in cols:
                    continue
                col = str(col.split('.'))[0]
                sql = 'select * from {} where {} {} {} limit 1'.format(table,col,opt,'"'+str(key)+'"')
                try:
                    if col in inv_cols:
                        raise Exception("InvalidCol")
                    if cur.execute(sql) > 0:
                        Sem_dict[key]['where'] = 1
                        Sem_dict[key]['where_col'] = str(table)+"."+str(col)
                        Sem_dict[key]['where_clause'] = ' {} {} {}'.format(col,opt,'"'+str(key)+'"')
                        found = 1
                        break
                except InvalidCol as e:
                    Not = "Noting"
                except:
                    print (sql,key,"didn't work","close cols")
                    found = 0
            if found == 0:
#                     cols.remove('total_offer')
#                     cols.remove('taxes')
#                     cols.remove('gift_packing_charges')
                for col in cols:
                    if col in DateCol or col in inv_cols:
                        continue
                    sql = "select * from {} where {} {} {} limit 1".format(table,col,opt,'"' + str(key) + '"')
                    try:
                        if cur.execute(sql) > 0:
                            Sem_dict[key]['where'] = 1
                            Sem_dict[key]['where_col'] = str(table)+"."+str(col)
                            Sem_dict[key]['where_clause'] = " {} {} {}".format(col,opt,'"'+str(key)+'"')
                            found = 1
                            break
                    except:
                        print (sql,"didn't work","bruteforce")
                        found = 0
                if found == 0:
                    if str(key) not in like_phrase:
                        if like_phrase == "":
                            like_phrase = str(key)
                        else:
                            like_phrase = like_phrase + " " + str(key)
    
    if found == 0:
        for table in table_list:
            cols = get_cols(table)
            for col in cols:
                if col in DateCol:
                    continue
                sql = "select * from {} where {} LIKE '%{}%' limit 1".format(table,col,like_phrase)
                if cur.execute(sql) > 0:
                    Sem_dict[key]['where'] = 1
                    Sem_dict[key]['where_col'] = str(table)+"."+str(col)
                    Sem_dict[key]['where_clause'] = " {} LIKE %{}% ".format(col,like_phrase)
                    found = 1
                    break


cols_score = {}
##


##    
def Semantic(word,nl = ""):
    det_cols = []
    pot_tables = []
    pot_cols = []
    Req = 1
    for table in table_list:
        tmp = table.split("_")
        for x in tmp:
            One = wn.synsets(word)
            Two = wn.synsets(x)
            Sim = 0.00
            if len(One) > 0 and len(Two)>0:
                Sim = One[0].wup_similarity(Two[0])
            if Sim is not None:
                if Sim >= 0.75:
                    print (word,"POT TABLE: ",table,"similarity: ",Sim)
                    pot_tables.append(table)
                    break
            if difflib.SequenceMatcher(None,table,word).ratio() >= 0.80:
                print ("POT TABLE: ",difflib.SequenceMatcher(None,table,word).ratio())
                pot_tables.append(table)
#         else:
#             print (item,word,"ratio is",difflib.SequenceMatcher(None,item,word).ratio())
        sql = "select * from {} limit 1".format(table)
        cur.execute(sql)
        cols = [x[0] for x in cur.description]
        for col in cols:
            if word == "order":
                break
            SimScore = 0.0
            Sim = 0.0
            ctr = 0
            tmp = col.split("_")
            for x in tmp:
#                 if x in sw:
#                     break
                if x == "total":
                    continue
                if x == "is":
                    continue
                One = wn.synsets(word)
                Two = wn.synsets(x)
                if len(One) > 0 and len(Two)>0:
                    Sim = One[0].wup_similarity(Two[0])
                if Sim is not None:
                    if Sim > 0.75:
                        det_cols.append(str(table+"."+col))
                    SimScore = SimScore + Sim
                ctr = ctr + 1
            if ctr == 0:
                SimScore = 0
            else:
                SimScore = SimScore / ctr
            cols_score[str(table)+"."+str(col)] = SimScore
            det_cols = remove_duplicates(det_cols)
            try:
                if Clean[Clean.index(word) - 1] in detail_list or Clean[Clean.index(word) + 1] in detail_list:
                    pot_cols = pot_cols + det_cols
                    Req = 0
                    print (word,"-->",col,"SimScore = ",SimScore)
            except:
                pass
            if SimScore > 0.85 and not(Req == 0):
                pot_cols.append(table+"."+col)
                Req = 0
                print (word,"-->",col,"SimScore = ",SimScore)
            tmp = col.replace("_"," ")
#             if tmp in word and not(word == "order" or word =="orders"):
#                 if (table+"."+col) not in pot_cols:
#                     print (word,"--> POT COL2:",col," direct nl match")
#                     pot_cols.append(table+"."+col)
            if difflib.SequenceMatcher(None,col,word).ratio() >= 0.80 and not(word == "order" or word =="orders"):
                print (word,"POT COL: ",col,"ratio: ",difflib.SequenceMatcher(None,col,word).ratio())
                pot_cols.append(table+"."+col)
    if pot_cols == [] and not (word == "total") and Req == 0:
        MaxCol = []
        for key in cols_score:
            if cols_score[key] == max(cols_score.values()):
                MaxCol.append(str(key))
            elif cols_score[key] >= 0.5:
                if cols_score[key] >= (max(cols_score.values()) - 0.25):
                    MaxCol.append(str(key))
        print ("FORCED")
        print (MaxCol)
        pot_cols = MaxCol
    try:
        pot_cols = remove_duplicates(pot_cols)
    except:
        pass
    return {word:{"pot_tables":pot_tables,"pot_cols":pot_cols,"where":0,"where_col":"","where_clause":"","date":0}}
##


raw_nl = input("Enter Query: ")
print ("\n\n\n")
nl = word_tokenize(raw_nl)
ps = PorterStemmer()
Clean = []
ChkSum = 0
ChkCt = 0
ChkAvg = 0
for i,words in enumerate(nl):
    if Lemma.lemmatize(words) == "name":
        Clean.append("name")
        continue
    if Lemma.lemmatize(words) == "detail":
        try:
            if Lemma.lemmatize(nl[i-1]) == "order":        
                continue
        except:
            pass
    if Lemma.lemmatize(words) == "select":
        continue
    if Lemma.lemmatize(words) == "invoice"  or Lemma.lemmatize(words) == "revenue" :
        Clean.append("revenue")
        ChkSum = 1
        continue
    if Lemma.lemmatize(words) == "count" or Lemma.lemmatize(words) == "number":
        ChkCt = 1
        continue
    if Lemma.lemmatize(words) == "average" or Lemma.lemmatize(words) == "avg": #or wn.synsets(Lemma.lemmatize(words))[0].wup.similarity(wn.synsets("average")[0]) > 0.8:
        ChkAvg = 1
        continue
    if Lemma.lemmatize(words) == "total" and "each" not in nl:#or wn.synsets(Lemma.lemmatize(words))[0].wup.similarity(wn.synsets("sum")[0]) > 0.8:
        ChkSum = 1
        continue
#     if Lemma.lemmatize(words) == "product":
#         Clean.append("order_items")
#         continue
    if Lemma.lemmatize(words) == "price":
        Clean.append("total_amount")
        Clean.append("amount")
        continue
    if "id" in words:
        Clean.append(words)
        continue
    if "name" in words:
        Clean.append(words)
        continue
    if "name" in words:
        Clean.append(words)
        continue
    if "date" in words:
        Clean.append(words)
        continue
    if "image" in words:
        Clean.append(words)
        continue
    if words not in sw:
        Clean.append(Lemma.lemmatize(words))
    if words == "total":
        continue
#     else:
#         print (words+" discarded")
print (Clean)
new_Clean = []
replaced_words = []
ChkTime = 1
while (ChkTime >= 1):
    for i,word in enumerate(Clean):
        ChkTime = ChkTime - 1
        if "id" in word:
            if Lemma.lemmatize(nl[nl.index('id') - 1]) in id_list:
                new_Clean.append(Clean[i-1]+'_'+word)
                replaced_words.append(nl[nl.index('id') - 1])
                ChkTime = ChkTime + 1
                continue
                #del Clean[i]
        if "amount" in word: 
            if Lemma.lemmatize(Clean[i-1]) in amount_list:
                new_Clean.append(Clean[i-1]+'_'+word)
                ChkTime = ChkTime + 1
                replaced_words.append(Clean[i-1])
                #del Clean[i]
            try:
                if Lemma.lemmatize(Clean[i+1]) in amount_list:
                    new_Clean.append(word+'_'+Clean[i+1])
                    replaced_words.append(Clean[i+1])
                    #del Clean[i]
            except:
                pass
            continue
        if "product" in word:
            if Lemma.lemmatize(nl[nl.index('product') - 1]) in product_list:
                print ("replacing:",Clean[i-1])
                new_Clean.append(word+'_'+Clean[i+1])
                print ("with",Clean[i-1])
                replaced_words.append(nl[nl.index('product') - 1])
                #del Clean[i]
                ChkTime = ChkTime + 1
            elif Lemma.lemmatize(nl[nl.index('product') + 1]) in product_list:
                print ("replacing:",Clean[i+1])
                new_Clean.append(word+'_'+Clean[i+1])
                print("with",Clean[i+1])
                replaced_words.append(nl[nl.index('product') + 1])
                #del Clean[i]
            continue
        new_Clean.append(word)
new_Clean = new_Clean + Clean
new_Clean = remove_duplicates(new_Clean)
for i,word in enumerate(new_Clean):
    new_Clean[i] = Lemma.lemmatize(word)
print (new_Clean)
for i,word in enumerate(new_Clean):
    if word in replaced_words:
        print (word,"removed")
        new_Clean.remove(word)
print (new_Clean)
for words in new_Clean:
    words = Lemma.lemmatize(words)
    if words == "order":
        Sem_dict.update({"order":{"pot_tables":["order_items","order_details"],"pot_cols":[],"where":0,"where_col":"","where_clause":""}})
    else:
        Sem_dict.update(Semantic(words,nl))
    #print (Sem_dict,"\n",words)
#print (Sem_dict)

where_clause = ""
pot_where = []
isWhere = False
for key in Sem_dict:
    if Sem_dict[key]['pot_tables'] == []:
        if Sem_dict[key]['pot_cols'] == []:
            pot_where.append(key)
        else:
            for col in Sem_dict[key]['pot_cols']:
                if col not in pot_where:
                    pot_where.append(col)
    else:
        for col in Sem_dict[key]['pot_cols']:
            if col not in pot_where:
                pot_where.append(col) 
#print (pot_where)

notCols = []
for table in table_list:
        sql = "select * from {} limit 1".format(table)
        cur.execute(sql)
        cols = [x[0] for x in cur.description]
        for item in pot_where:
            if item not in cols:
                isWhere == True
                notCols.append(item)
CheckWhere(nl)
date_keys = []
for key in Sem_dict:
    if Sem_dict[key]['where'] == 1 and Sem_dict[key]['where_clause'] == "":
        print ("Checking",key,"for Date")
        if CheckDate(str(key)) is not None:
            date_keys.append(str(key))
            
if len(date_keys) == 1:
    for date in date_keys:
        Dates = CheckDate(date)
        Sem_dict[date]['where_clause'] = 'created_date >= {} and created_date <= {}'.format('"'+str(Dates[0])+'"','"'+str(Dates[1])+'"')
elif len(date_keys) > 1:
    #if True: ## in between date ranges
        
    Final_date = ""
    for date in date_keys:
        del Sem_dict[date]
        Final_date = Final_date+str(date)
    Dates = CheckDate(Final_date)
    Sem_dict[Final_date] = {'pot_tables':[],'pot_cols':[],'where_clause':'created_date >= {} and created_date <= {}'.format('"'+str(Dates[0])+'"','"'+str(Dates[1])+'"')}
print (date_keys)            
print (Sem_dict)
final_cols = ""
final_tables = []
for key in Sem_dict:
    for table in Sem_dict[key]['pot_tables']:
        if table not in final_tables:
            print ("final table: ",table)
            final_tables.append(table)
    
    for col in Sem_dict[key]['pot_cols']:
        table = col.split(".")[0]
        if table not in final_tables:
            final_tables.append(table)
            print ("final table: ",table)
print ("\n\n")
for table in final_tables:
    where_clause = ""
    for key in Sem_dict:
        if not(Sem_dict[key]['where_clause'] == ""):
            if where_clause == "":
                where_clause ="where "+ str (Sem_dict[key]['where_clause'])
            else:
                where_clause = where_clause + " and " + str(Sem_dict[key]['where_clause'])
        for col in Sem_dict[key]['pot_cols']:
            if col.split(".")[0] == table:
                if final_cols == "":
                    final_cols = '"'+col+'"'
                else:
                    final_cols = final_cols + "," + '"'+col+'"'
    if final_cols == "" and QE == 0:
        final_cols = "*"
    if final_cols == "":
        break
    if ChkCt == 1:
        final_cols = "count(" + final_cols +")"
    elif ChkAvg == 1:
        final_cols = "avg(" + final_cols +")"
    elif ChkSum == 1:
        final_cols = "sum(" + final_cols + ")"
#     print ("\nCHECK SUM",ChkSum)
#     print("\n",final_cols)
    print ("select {} from {} {}".format(final_cols,table,where_clause))
    final_cols = ""
    QE = 1

fp.close()
con.close()
