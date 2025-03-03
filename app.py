from flask import Flask, render_template, request, redirect, url_for, session
import pandas as pd
import psycopg2
import google.generativeai as genai
import os

app = Flask(__name__)

secret_key=os.getenv('GEMINI_KEY')
psg_pswd=os.getenv('psg_pswd')
psg_endpoint=os.getenv('psg_endpoint')
genai.configure(api_key=secret_key)

def clean_sql_query(sql_query):
    return sql_query.replace("```sql", "").replace("```", "").strip()

def clean_html_query(sql_query):
    return sql_query.replace("```html", "").replace("```", "").strip()

# Define metadata context for your table
metadata_context = """
Table: aiops.haryana_risk_data
Columns:
    month integer,
    ndvi real,
    soilmoisture real,
    rainfall real,
    temperature real,
    windspeed real,
    humidity real,
    soil_ph real,
    historicalrisk real,
    croptype text,
    riskscore real

Default Behavior:
  - If not specified, sort by RiskScore in descending order.
  - If crop type is missing, consider all crops.
  - Always use lowercase column name strictly
  - Use aiops.haryana_risk_data strictly as table name while generating queries.
  - Note: croptype values can be 'Wheat', 'Rice', 'Cotton', 'Mustard', 'Sugarcane'
"""

def generate_sql_with_explanation(user_query, extra_context=""):
    prompt = f"""{metadata_context}
        User Query: "{user_query}"
        {extra_context}
        Please provide:
        1. A SQL query that returns the matching entries. if no count is mentioned, limit 3
        2. A plain English explanation of what the SQL query does 
        3. Key aspects to analyze in the results 

        Note: Do not mention table name ever in sql explanation or analysis.
        Note: Always generate sql query compatible with PostgreSQL
        
        Format your response as:
        QUERY: <the SQL query>
        EXPLANATION: <plain English explanation>
        ANALYSIS_POINTS: <key points to analyze>
        """
    
    model = genai.GenerativeModel('gemini-2.0-flash')
    response = model.generate_content(prompt)
    
    # Parse the response
    parts = response.text.split('\n')
    sql_query = ""
    explanation = ""
    analysis_points = ""
    
    current_section = ""
    for line in parts:
        if line.startswith("QUERY:"):
            current_section = "query"
            continue
        elif line.startswith("EXPLANATION:"):
            current_section = "explanation"
            continue
        elif line.startswith("ANALYSIS_POINTS:"):
            current_section = "analysis"
            continue
            
        if current_section == "query":
            sql_query += line.strip() + " "
        elif current_section == "explanation":
            explanation += line.strip() + " "
        elif current_section == "analysis":
            analysis_points += line.strip() + " "
    
    return {
        "query": clean_sql_query(sql_query),
        "explanation": explanation.strip(),
        "analysis_points":clean_html_query(analysis_points.strip())
    }

def analyze_results(df, analysis_points, user_query):
    data_str = df.to_string()
    prompt = f"""
    Given the following data and analysis points, provide a comprehensive analysis in html format not more than 200 words:
    
    User Query: {user_query}
    Analysis Points: {analysis_points}
    
    Data Values:
    {data_str}
    
     Please provide a detailed analysis including:
    1. Specific observations from the data values shown
    2. Key patterns and trends in the actual numbers
    3. Notable relationships between different columns
    4. Practical implications based on the exact values
    5. Specific recommendations based on these data points
    
    Format your response with clear headings and bullet points.
    Use actual values from the data to support your analysis.
    """
    
    model = genai.GenerativeModel('gemini-2.0-flash')
    response = model.generate_content(prompt)
    return response.text

def execute_sql_query(sql_query):
    conn = psycopg2.connect(
        dbname="postgres",      # Replace with your database name
        user="postgres",             # Replace with your username
        password=psg_pswd,         # Replace with your password
        host=psg_endpoint,     # Replace with your DB endpoint
        port="5432"                  # Default PostgreSQL port
    )
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        cursor.close()
        df = pd.DataFrame(results, columns=column_names)
        return df
    except Exception as e:
        print("SQL Execution Error:", e)
        return None
    finally:
        conn.close()

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/generate", methods=["POST"])
def generate():
    user_query = request.form.get("user_query")
    session["user_query"] = user_query
    sql_info = generate_sql_with_explanation(user_query)
    session["sql_info"] = sql_info
    return render_template(
        "result.html",
        sql_query=sql_info["query"],
        sql_explanation=sql_info["explanation"],
        analysis_points=sql_info["analysis_points"],
        user_query=user_query
    )

@app.route("/refine", methods=["POST"])
def refine():
    user_query = session.get("user_query", "")
    action = request.form.get("action")
    
    if action == "modify":
        extra_context = request.form.get("feedback", "")
        sql_info = generate_sql_with_explanation(user_query, extra_context)
        session["sql_info"] = sql_info
        return render_template(
            "result.html",
            sql_query=sql_info["query"],
            sql_explanation=sql_info["explanation"],
            analysis_points=sql_info["analysis_points"],
            user_query=user_query
        )
    elif action == "confirm":
        sql_info = session.get("sql_info", {})
        df_results = execute_sql_query(sql_info["query"])
        
        if df_results is not None:
            analysis = analyze_results(df_results, sql_info["analysis_points"], user_query)
            return render_template(
                "results.html",
                results=df_results,
                sql_query=sql_info["query"],
                sql_explanation=sql_info["explanation"],
                analysis=clean_html_query(analysis)
            )
    
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run()