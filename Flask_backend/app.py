from dotenv import load_dotenv
import psycopg2 as psy
import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import google.generativeai as genai

load_dotenv()

app = Flask(__name__)
cors = CORS(app)

genai.configure(api_key=os.getenv("GOOGLEAPI"))

#Phase 1 which involves providing the LLM with NL based query and asking it to return an SQL Query
def get_gemini_response(question):
    prompt1=""" You are an expert data analyst and english major. The user will give the query to the database and read the output but the user lacks knowledge of sql syntax. You specialize in converting English based database query to SQL query for a particular table in the database. 
    You dont give reasons to anything but you only reply with sql queries. The output after the sql query is given to the database will be read by someone else to reply to users question regarding trend analysis.
    You have a special way of return the sql query. You always add "```"sql in the start of the query and end the query with "```" as an output.
    Ensure to add the the prefix and suffix to the query always.
    prefix: ```sql
    suffix: ```

    Being a data analyst you are some times asked to analyse ro do trend analysis certain parts of the database.
    You dont provide with the analysis rather you just generate an sql query that shows all the records that satisy that constraint
    For example,
    during summer which item was sold the most here you assume data range according to your own like may to august 
    then you reply with an sql query:
    SELECT product, quantitiy FROM SALES WHERE MONTH>=5 AND MONTH<=8 DESC LIMIT 5;

    Dont directly give the maximum give top 5 or something like that depending on the question so that the data analyst ahead could give a better reply. 
    If needed also include other columns which you think could help in better analysis.

    Let the user see the data and decide for themselves you dont give reason but only a query with your prefix and suffix.
    Another analyst will assess the question, query, and output and give the answer.

    The user could also ask for predicitons just genrate a query to get past data and let the other analyst asses the queries to give an output to the users question.
    Your job is to only generate sql query with your prefix and suffix.


    You could be asked to compare between the sales of 2 products in the month of september then you provide a query that give information about both product in semptember.
    You might be asked why are most products bought during 12 PM to 5PM in december.
    Then you return with query that shows the number of products bought in every hr for the month of december.
    Then later the output from the query will be analysed by someone else who can give reply to the user.

    This part requires you to assume values sometimes according to you own because the user might ask you questions that use vague or generic terms that lack precise information.
    Reply only with the SQL query and make sure to add your prefix and suffix to the SQL query.

    If the user says "hi" or asks other friendly conversational questions. Respond politely and remind the user that you can only help with sql related doubts.

    You have information about the sales table's metadata. The table_schema name is 'public' and the table_name is 'sales'.
    You can also be asked questions regarding the schema and it will be explicitly mentioned in the question. The term schema stands for the meta data like the names of colums in the table and their data type.
    The SQL table has the name SALES and has the following columns:
    srno, orderId, product, quantity, priceIndividual, date, address, month, priceTotal, City, Hour. Date is of the format yyyy/mm/dd. Month column has integer values representing the month number like december as 12. Hour is in 24 hour format.
    priveIndividual is the price of 1 quantity of that product and priceTotal is the price of total price for the quantities purchased.

    For constraints where city like or product= is used mention the value in '' quotes and use % as suffix and prefix in the query
    For example City LIKE '%Los Angeles%'

    Follow sql syntax.
    
    \n\nFor example,
    \nExample 1 - 
    Question:How many entries of records are present?, 
    OUTPUT: SELECT COUNT(*) FROM SALES ;
    \nExample 2 - 
    Question:Tell me all the products bought on 2019/12/25?, 
    OUTPUT: SELECT DISTINCT PRODUCT FROM SALES where DATE='2019/12/25';
    \nExample 3 -  
    Question:Give me total count of all products bought on 2019/12/25, 
    OUTPUT: SELECT SUM("quantity") FROM SALES where DATE='2019/12/25'; 

    \nExample 4 -  
    Question:Give me the schema/script/metadata for the table sales
    OUTPUT: SELECT column_name,data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'sales';

    Here, give a reponse for the question asked:
    """
    model = genai.GenerativeModel('gemini-1.5-pro-latest')
    response =model.generate_content([prompt1,question])
    return response.text 

#Phase 2 of reading the SQL Query generated by the LLM
def read_sql_query(sql):
    try:
        conn = psy.connect(
            dbname=os.getenv("DBNAME"),
            user=os.getenv("DBUSER"),
            password=os.getenv("DBPASS"),
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        conn.commit()
        conn.close()
        print(rows)
        return rows
    except (Exception, psy.Error) as error:
        return f"Error reading sql query python backend: {error}"

#Phase 3 of generating NL response according to phase 1 and phase 2 
def nl_response(input, result_tuple, sql_query):
    prompt2 = '''\You are a data analysis expert and a friend of the user. You are also very smart and can provide good logical reasoning. You are phenomenal in taking the follwing as inputs: question, sql query which was sent to postgresql server, and output from the postgresql server using the query and returning a natural language reply based on output.
    The user approaches you with either a conversation like a friend or some sql query question.
    Mostly it is sql query based but there could be conversational question asked. Like "how are you?" to which you try to continue the conversation but also remind that you are a master of data analysis and if there is anything related to that the user might want to ask.
    The output generated should have context of the question asked by the user for which the SQL response was generated. The response generated is a python tuple.

    Here is the schema for you reference:
    The SQL table has the name SALES and has the following columns:
    srno, orderId, product, quantity, priceIndividual, date, address, month, priceTotal, City, Hour. 
    Date is of the format yyyy/mm/dd. Month column has integer values representing the month number like december as 12. 
    Hour is in 24 hour format and denotes a range of that hour to 1 minute before the next hour.
    For example if the purchase hours is 8 that means the purchase was done between 8:00 AM to 8:59 AM. if the purchase hours is 18 that means the purchase was done between 6:00 PM to 6:59 PM.So reply by converting the answer into 12hr format i.e. if the value is greater than 12 then it would be PM else AM
    priveIndividual is the price of 1 qunitty of that product and priceTotal is the price of total price for the quantities purchased.

    Being data analysis expert, you are also asked question regarding trend analysis. 
    You look at the question and the response and then generate a reply after analysing the data.
    For example,
    during summer which item was sold the most here you assume data range according to your own like may to august 
    the query was
    SELECT * FROM SALES WHERE MONTH>=5 AND MONTH<=8;
    The data shows items like icecream, AAA batteries, Sweater. But the item which was sold the most was icecream.
    So you provide a logical reasoning to why the answer could be icecream during htat time along with a natural language response that icecream was the most sold item during summer.
    Try finding some data if it is related to money, popularity, temperature or scientific reasons etc for that particular result. 
    Use some percenatages, of statistical methods like average mode in reply at times along with the reply.
    If more data is needed mention what other data could affect the answer.
    
    If the answer has multiple values like when items are to be listed then the tuple contains multiple tuples where each inner tuple has an item and maybe associated values.
    If the answer has a single tuple maybe it returns count of the entries or sum of some value or maybe it could be that only 1 record exists of given constraints
    So traverse the result accordingly, extract the values and try to generate natural language output accordingly.


    Important: If the sql output was "Error reading sql query python backend:"  then read the question.
    If the question asked by the user was conversational. 
    Like if the question was "hi" or other friendly conversational questions. Respond politely by answering that question and also esnure to remind the user that you can only help with sql related doubts.
    If it was sql related then reply with "Sorry, I am unable to generate a response from the above query. Please check the input again or try again later."

    The output format should just have reply in natural language that a human can read without bold or italics or underline if pointer need '-' would suffice.

    \nFor example, 
    \nExample 1- Question: How many entries of records are present?
    sql output: ((1500))
    There are 1500 records in the table.

    Example 2-  Question: Tell me all the products bought on 2019/12/25?,
    sql output: (('20in Monitor', 26),('27in 4K Gaming Monitor', 25), ('Apple Airpods Headphones', 70))
    You reply with:
    Products bought on 2019/12/25 are: \n
    20in Monitor - 26,\n
    27in 4K Gaming Monitor - 45,\n
    Apple Airpods Headphones -  70\n

    Example 3- Question: Hi how are you?
    sql output: Error reading sql query python backend:
    You have assessed that the output has sql error so either there is error in the query or it is completely irrelevant to sql. You find it is conversational so you reply.
    Hello. I am good what about you?
    

    Your turn:
    Question:{input}
    sql output: {result_tuple}
    \
    '''.format(input = input, result_tuple = result_tuple, sql_query =sql_query)

    # print(prompt2)
    
    model = genai.GenerativeModel('gemini-1.5-pro-latest')
    response = model.generate_content([prompt2])
    # print(response)
    return response.text

@app.route('/generate-query', methods=['POST'])
# if request.method == 'POST':
def input():
    data = request.get_json()
    user_ip = data.get('input')
    print(type(user_ip))
    print("I/P:"+user_ip)

    if user_ip=="":
        return "Input data missing'", 400
    
    sql_query = get_gemini_response(user_ip)
    print(sql_query)
    trim_sql_query = sql_query[6:-4]
    # schema_query = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'sales' ;"
    result_tuple = read_sql_query(trim_sql_query)
    print(result_tuple)
    final_result = nl_response(user_ip,result_tuple, trim_sql_query);
    print ("Response by the 2nd NL:" + final_result)

    # return jsonify({'result': result_tuple}), 200
    return final_result, 200