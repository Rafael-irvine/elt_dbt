import requests
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv
import os

load_dotenv()

class JobicyAPI:
    # Definindo os atributos da classe
    def __init__(self,base_url:str, industry:str, count:int):
        self.base_url = base_url
        self.industry = industry
        self.count = count
        self.data = None
        
    # Método para requisição dos dados da API e salvar num atributo da propria classe
    def fetch_data(self):
        url = f"{self.base_url}?count={self.count}&industry={self.industry}"
        response = requests.get(url)
        if response.status_code == 200:
            self.data = response.json()
        else:
            response.raise_for_status()
            
    # Métod para carregar dados do self.data em um dataframe
    def get_jobs_data(self):
        if self.data and 'jobs' in self.data:
            return pd.DataFrame(self.data['jobs'])
        else:
            return pd.DataFrame()
# Classe para conectar e salvar dados no snowflake
class Snowflake:
    def __init__(self,account:str, user:str, password:str, database:str, schema:str, warehouse:str):
        self.engine = create_engine(URL(
            account = account,
            user = user,
            password = password,
            database = database,
            schema = schema,
            warehouse = warehouse,
        ))
        
    # Salvando dados no snowflake a partir de um dataframe
    def save_snowflake(self,df: pd.DataFrame, table_name: str):
        df.to_sql(table_name,self.engine, if_exists='replace', index=False)

def main():
    api = JobicyAPI(
        base_url = "https://jobicy.com/api/v2/remote-jobs",
        industry = "data-science",
        count = 10
    )
    api.fetch_data()
    
    jobs_df = api.get_jobs_data()
    
    # Verificando se o dataframe não esta vazio 
    if not jobs_df.empty:
        
        # Ajustando nome das colunas
        jobs_df.columns =[
            "ID", "URL", "jobSlug", "jobTitle", "companyName", "companyLogo",
            "jobIndustry", "jobType", "jobGeo", "jobLevel", "jobExcerpt",
            "jobDescription", "pubDate", "annualSalaryMin", "annualSalaryMax", "salaryCurrency","salaryPeriod"]
        
        # Alinhando tipos de dados
        jobs_df["ID"] = jobs_df["ID"].astype(int)
        
        
        numeric_columns = ["annualSalaryMin", "annualSalaryMax"]
        
        for numeric in numeric_columns:
            jobs_df[numeric] = pd.to_numeric(jobs_df[numeric], errors='coerce')

        string_columns = ["URL", "jobSlug", "jobTitle", "companyName", "companyLogo",
            "jobIndustry", "jobType", "jobGeo", "jobLevel", "jobExcerpt",
            "jobDescription", "pubDate","salaryCurrency","salaryPeriod"]

        for column in string_columns:
            jobs_df[column] = jobs_df[column].astype("string")
            
                              
    
        
        save= Snowflake(
            account = os.getenv("ACCOUNT"),
            user = os.getenv("USER"),
            password = os.getenv("PASSWORD"),
            database = os.getenv("DATABASE"),
            schema = os.getenv("SCHEMA"),
            warehouse = os.getenv("WAREHOUSE"),
            )
        save.save_snowflake(jobs_df,table_name="listagem_trabalhos_remotos")
        print("Dados salvos com sucesso no Snowflake")
    else:
        print("Não existem dados a serem salvos no Snowflake")
        
if __name__ == '__main__':
    main()
    
                    

        
        
        