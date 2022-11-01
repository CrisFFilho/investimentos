import psycopg2
import psycopg2.extras
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import datetime

def query(db_connection, dql_statement):
    cur = db_connection.cursor()
    cur.execute(dql_statement)
    return cur

def commit_transaction(db_connection):
    db_connection.commit()

    
def open_cnn():
    return psycopg2.connect(
        host=PGSQL_HOST,
        user=PGSQL_USER,
        password=PGSQL_PASS,
        dbname=PGSQL_DATABASE)

PGSQL_HOST = 'localhost'
PGSQL_USER = 'postgres'
PGSQL_PASS = 'masterkey'
PGSQL_DATABASE = 'postgres'

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")

path_default = r"C:\Users\cristian.fracalossi\Desktop\ChromeDriver"

hoje = datetime.date.today()

browser = webdriver.Chrome(executable_path=(path_default + '\chromedriver.exe'), chrome_options = options)   
browser.get("https://www.fundsexplorer.com.br/funds")
for i in range(334):
    dados_banco = []
    if i > 0:
        try:
            browser.get("https://www.fundsexplorer.com.br/funds")
            fii = browser.find_element_by_xpath(f'//*[@id="fiis-list-container"]/div[{i}]')
            sigla_fii = fii.text.split("\n")
            browser.get(f"https://www.fundsexplorer.com.br/funds/{sigla_fii[0]}")

            nome_completo_fii = browser.find_element_by_class_name('section-subtitle')
            price_div = browser.find_element_by_id('stock-price')
            valor = price_div.find_element_by_class_name('price')
            percentual = price_div.find_element_by_class_name('percentage')


            div_valores_gerais = browser.find_element_by_id('main-indicators-carousel')
            valores_gerais = div_valores_gerais.find_elements_by_class_name('carousel-cell')

            dados_banco.append([sigla_fii[0], nome_completo_fii.text, valor.text, percentual.text])

            for i in range(7):
                texto_sem_espaco = valores_gerais[i].text.split('\n')
                dados_banco[0].append(texto_sem_espaco[1])

            dados_banco[0].append(hoje)

            db_con = open_cnn()
            insert = db_con.cursor()
            sql = r"insert into fiis (sigla, nome, valor, percentual_na_data, liquidez_diaria, ultimo_rendimento, dividend_yield, patrimonio_liquido, valor_patrimonial, rentabilidade_no_mes, p_vp, data_hora) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            psycopg2.extras.execute_batch(insert, sql, dados_banco)
            db_con.commit()
            db_con.close()
        except:
            print('Deu Erro')
        
