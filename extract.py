import pandas as pd
import string
import os

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common import TimeoutException

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

URL_BASE = "https://statusinvest.com.br/fundos-de-investimento/"
FUNDO_ANGA = "anga-credito-estruturado-fic-fim-cred"
BASE_PATH = os.path.dirname(os.path.abspath(__file__))  # current OS dir


def extract_html(fund_url):
    driver = webdriver.Firefox()
    try:
        cur_fund_url = URL_BASE + fund_url
        driver.implicitly_wait(20)  # seconds
        driver.get(cur_fund_url)
        span_locator = (
            By.CSS_SELECTOR,
            'span[class="fs-2 fs-xs-3 ml-1 d-flex align-items-center "]')

        # Get the initial text inside the span
        initial_text = driver.find_element(*span_locator).text
        print(initial_text)

        # Wait for the text inside the span of the best and worst months
        # to change
        try:
            WebDriverWait(driver, 35).until(lambda driver: driver.find_element(
                *span_locator).text != initial_text)

        except TimeoutException as e:
            print("The month text did not render")
            print(f"TimeoutException: {e}")
    finally:
        # Close the web driver
        html_code = driver.page_source
        print(html_code)
        driver.quit()

    # # Prettify the HTML with bs4
    soup = BeautifulSoup(html_code, 'html.parser')
    pretty_html = soup.prettify()
    print(pretty_html)
    return soup


def parse_url_to_fund(fund_url):
    """
     Parses the provided URL for fund information and creates a DataFrame.

     Parameters:
     - url (str): The URL part of the specific fund information page.

     Returns:
     - pd.DataFrame: A DataFrame containing parsed fund information.
     """
    # Create a new Chrome WebDriver instance with the specified path

    soup = extract_html(fund_url)
    main_header = soup.find('div', id='main-header-info').find_all('strong',
                                                                   class_="value")

    rentability_12m = main_header[0].text.strip()
    rentability_24m = main_header[1].text.strip()
    volatilidade = main_header[2].text.strip()
    patrimonio_liquido = main_header[4].text.strip()
    preco_da_cota = main_header[5].text.strip()
    valor_inicial_minimo = soup.find("div",
                                     class_=
                                     "top-info top-info-1 top-info-sm-2 top-info-md-3 top-info-xl-n sm d-flex justify-between").find(
        "strong", class_="value").text.strip()

    taxas_do_fungo_div = soup.find('div',
                                   class_="top-info top-info-1 top-info-sm-2 top-info-md-3 top-info-xl-n sm d-flex justify-between").find_all(
        "strong", class_=
        "value")

    taxa_de_administracao = taxas_do_fungo_div[0].text.strip()
    taxa_de_performance = taxas_do_fungo_div[1].text.strip()
    cnpj_number = soup.find('div',
                            class_='d-block d-md-flex mb-5 img-lazy-group').find_all(
        'h5')[1].text.strip()
    Razao_social = soup.find('div',
                             class_='d-block d-md-flex mb-5 img-lazy-group').find(
        'span', class_="d-block fw-600 text-main-green-dark").text.strip()


    # best and worst month
    # months = soup.find('div', class_="card card-body p-sm-2 p-xl-3").findAll(
    #     'span', {'data-item': 'fundBestMonthPositive'})
    # class_='fs-2 fs-xs-3 ml-1 d-flex align-items-center')
    # best_month = months[0].find('span').text.strip()
    # worst_month = months[1].find('span').text.strip()

    # Criar um DataFrame pandas com os dados extraídos
    data = {
        'Rentabilidade 24 meses': [rentability_12m],
        'Rentabilidade 12 meses': [rentability_24m],
        'Volatilidade': [volatilidade],
        'Patrimonio Liquido': [patrimonio_liquido],
        'Preco da Cota': [preco_da_cota],
        'Valor Inicial Minimo': [valor_inicial_minimo],
        'Taxa de Administração': [taxa_de_administracao],
        'Taxa de Performance': [taxa_de_performance],
        'Razao social': [Razao_social],
        'CNPJ': [cnpj_number]
        # ,'Melhor Mês': [melhor_mes],
        # 'Pior Mês': [pior_mes]
    }

    df = pd.DataFrame(data)
    raw_data_path = os.path.join(BASE_PATH, 'Raw\\raw_data.csv')
    df.to_csv(raw_data_path, index=False)
    return df


def transform(df):
    """
    Applies transformations to the DataFrame, and saves it to "Trusted" folder
    :param df: the DF with the extracted data
    :return: the transformed DF
    """

    # Assuming 'CNPJ' is a string column, remove punctuation characters
    df['CNPJ'] = df['CNPJ'].apply(lambda x: ''.join(
        char for char in x if char not in string.punctuation))

    # Exemplo de transformação: Converter a coluna "Patrimonio Liquido" para float
    df['Patrimonio Liquido'] = df['Patrimonio Liquido'].apply(
        lambda x: float(x.replace(' B', '').replace(',', '.')))

    # Exemplo de transformação: Converter a coluna "Preço da Cota" para float
    df['Preco da Cota'] = df['Preco da Cota'].apply(
        lambda x: float(x.replace(',', '.')))

    # Exemplo de transformação: Converter a coluna "Rentabilidade" para float
    df['Rentabilidade 24 meses'] = df['Rentabilidade 24 meses'].apply(
        lambda x: float(x.replace(',', '.')))
    df['Rentabilidade 12 meses'] = df['Rentabilidade 12 meses'].apply(
        lambda x: float(x.replace(',', '.')))

    # Exemplo de transformação: Converter a coluna "Volatilidade" para float
    df['Volatilidade'] = df['Volatilidade'].apply(
        lambda x: float(x.replace(',', '.')))
    transformed_path = os.path.join(BASE_PATH, 'Trusted\\transformed_data.csv')
    # Salvar os dados transformados na pasta Trusted
    df.to_csv(transformed_path, index=False)
    return df


def load(df_transformed, df_supplementary_file):
    """
    merging the data of the transformed and the given Excel file,loading into a
    new csv named "final_data.csv" on the Refined folder
    :param df_transformed:  the transformed Pandas data frame
    :param df_supplementary_file: pandas data frame of the Excel file
    :return:
    """
    # code de cruzamento+ definir o df
    df_transformed['CNPJ'] = df_transformed['CNPJ'].astype(str)
    df_supplementary_file['CNPJ'] = df_supplementary_file['CNPJ'].astype(str)
    df_transformed = df_transformed.drop('Razao social', axis=1)
    df_final = pd.merge(df_supplementary_file, df_transformed, on='CNPJ',
                        how='inner')

    df_final = df_final.drop('Razao social', axis=1)  # duplicated
    df_final['Performance tax over net worth'] = df_final['TX_PFEE'] / \
                                                 df_final['Patrimonio Liquido']
    df_final['Adm tax over net worth'] = df_final['TAXA_ADM'] / df_final[
        'Patrimonio Liquido']

    final_path = os.path.join(BASE_PATH, 'Refined\\final_data.csv')
    df_final.to_csv(final_path, index=False)


# direct script:
if __name__ == '__main__':
    # call parse_url_to_fund with ANGA's fund suffix
    scraped_data = parse_url_to_fund(FUNDO_ANGA)

    # transform
    transformed = transform(scraped_data)
    os.path.join(BASE_PATH, 'Raw\\INFO_FUNDOS_ANGA_202307.xlsx')
    supplementary_file_path = os.path.join(BASE_PATH,
                                           'Raw\\INFO_FUNDOS_ANGA_202307.xlsx')
    supplementary_file_df = pd.read_excel(supplementary_file_path)

    # load
    load(transformed, supplementary_file_df)
