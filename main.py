import os
from atlassian.confluence import Confluence
import urllib3
from bs4 import BeautifulSoup
import re

# Отключение проверки SSL в urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
class ParserDDL:

    def __init__(self, path=None):
        self.url = 'https://confluence.dev.ic'
        self.confluence = None
        self.path = path


    def get_connection(self):
        """
        Создаем подключение к Confluence
        :return:
        """
        self.confluence = Confluence(
            url=self.url,
            username="********",
            password="*******",
            verify_ssl=False
        )
    def get_list_id(self, page):
        """
        :param page:
        :return:
        """
        projects_id = []

        titles = self.confluence.get_child_id_list(page)
        for title in titles:
            projects_id.append(title)

        return projects_id


    def get_child_list_id(self, list_id):
        """
        :param self:
        :param list_id:
        :return:
        """
        pages_id = []
        while len(list_id) != 0:
            titles = self.dfs(list_id)
            for title in titles:
                pages_id.append(title)
            list_id = titles

        return pages_id




    def get_list_titles(self, pages_id):
        """

        :param pages_id:
        :return:
        """
        title_list_child = []
        for page_id in pages_id:
            new = {self.confluence.get_page_by_id(page_id)['title']: page_id}
            title_list_child.append(new)
        print(title_list_child)

        return title_list_child


    def dfs(self, list_id):
        """
        Проводим глубинный поиск по сайту
        :param list_id:
        :return:
        """
        pages = []
        for page_id in list_id:
            page = self.confluence.get_child_id_list(page_id)
            if len(page) != 0:
                for p in page:
                    pages.append(p)
        return pages


    def save_protocol_version(self, version_data, version_folder):
        '''
        Сохраняем версию протокола
        :param version_data:
        :param version_folder:
        :return:
        '''

        if not os.path.exists(version_folder) and version_data is not None:
            os.makedirs(version_folder)

        # Сохраняем информацию о скрипте в файл script.sql
        if version_data is not None:
            with open(os.path.join(version_folder, "script.sql"), "w", encoding='utf-8') as script_file:
                script_file.write(version_data)
        else:
            print('Версия данных (version_data) равна None. Ничего не записано в файл.')


    def clean_project_name(self, project_name):
        """

        :param project_name:
        :return:
        """
        # Очищаем название проекта от недопустимых символов
        cleaned_name = re.sub(r'[\/:*?"<>|]', '', project_name)
        return cleaned_name


    def extract_script_from_page(self, html_content):
        """

        :param html_content:
        :return:
        """
        # Создаем объект BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Ищем тег
        div_tag = soup.find('ac:plain-text-body')

        # Если тег найден, извлекаем текст между ними
        if div_tag:
            extracted_text = div_tag.get_text()
            return extracted_text
        else:
            print('Протокол тестирования отсутствует')
            return None

    def get_page_data(self, html_content):
        # Основная функция для извлечения данных со страницы
        script = self.extract_script_from_page(html_content)
        return script



    def process(self, page_id):
        '''
        :param page_id:
        :return:
        '''
        # Создание словаря для хранения данных по проектам
        project_data = {}

        projects_id = self.get_list_id(page_id)
        projects_subpages_id = []
        protocol_versions = []
        for project_id in projects_id:
            project_subpage_id = self.get_list_id(project_id)
            projects_subpages_id.extend(project_subpage_id)

        title_projects = self.get_list_titles(projects_id)
        title_subpages = self.get_list_titles(projects_subpages_id)

        for data in title_subpages:
            title_subpage = list(data.keys())[0]  # ПОЛУЧАЕМ НАЗВАНИЯ ВЕРСИЙ ПРОТОКОЛОВ ТЕСТИРОВАНИЯ
            id_subpage = list(data.values())
            print(id_subpage)
            if 'Протоко' in title_subpage:
                version = self.get_child_list_id(id_subpage)
                print(version)
                protocol_versions.extend(version)

        title_protocol_versions = self.get_list_titles(protocol_versions)


        for data in title_projects:
            project_name = list(data.keys())[0]  
            number_project = project_name.split(' ', 1)[0] + ' '  
            cleaned_project_name = self.clean_project_name(project_name)  
            project_data[cleaned_project_name] = []  

            # Итерируемся по списку подстраниц 
            for version_data in title_protocol_versions:
                child_title = list(version_data.keys())[0]  
                if number_project in child_title:
                    project_data[cleaned_project_name].append({child_title: version_data[
                        child_title]}) 

        for project_name, versions in project_data.items():
            for version_data in versions:
                version_title = list(version_data.keys())[0]
                version_id = version_data[version_title]

                # Извлекаем информацию о странице
                page_content = self.confluence.get_page_by_id(version_id, expand="body.storage")["body"]["storage"][
                    "value"]
                page_data = self.get_page_data(page_content)

                # Создаем папку для версии протокола с очищенным именем проекта
                version_folder = os.path.join(self.path, project_name, version_title)

                # Сохраняем информацию о скрипте
                self.save_protocol_version(page_data, version_folder)

                print(f"Saved data for {project_name} - {version_title} to {version_folder}")

parser = ParserDDL(path="D:\\TEST")
parser.get_connection()
parser.process('167084041')
=======
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import logging


class FileProcessor:
    def __init__(self, dir_in, dir_out, dir_save):
        '''

        :param dir_in: source directory
        :param dir_out: final directory
        :param dir_save: intermediate directory
        '''
        # Constructor to initialize directory paths and logger
        self.dir_in = dir_in
        self.dir_out = dir_out
        self.dir_save = dir_save
        self.logger = self.setup_logger()

    def setup_logger(self):
        """

        :return: logger
        """
        # Initialize logger
        logger = logging.getLogger('FileProcessor')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def get_file_names(self):
        """

        :return: list of file names
        """
        # Get a list of file names in the specified directory
        command = f'hdfs dfs -ls {self.dir_in}'
        try:
            output = subprocess.check_output(command, shell=True)
            files = output.decode().split('\n')
            files_name = []
            for path in files:
                if path.endswith('.csv') or path.endswith('.csv.gz'):
                    files_name.append(path.split('/')[-1])
            return files_name
        except subprocess.CalledProcessError as e:
            self.logger.error(f'Ошибка выполнения команды {e}', exc_info=True)
            return []

    def change_file(self, csv_file, column_value, spark):
        '''
        Add a column and fill it with the specified value
        :param self:
        :param csv_file: file to process
        :param column_value: value to add to the column
        :param spark: SparkSession object for reading the file
        :return:
        '''

        df_csv = spark.read.csv(csv_file, header=True, sep=',', inferSchema=True)
        df_csv = df_csv.withColumn('dispatcher_id', lit(column_value))
        df_csv.coalesce(1) \
            .write \
            .option('sep', ',') \
            .mode('overwrite') \
            .option('header', True) \
            .option('compression', 'gzip') \
            .csv(self.dir_save)

    def get_random_file_name(self):
        '''
        Get the name of the processed file for renaming
        :param self:
        :return: name of the processed file
        '''

        command_save = f'hdfs dfs -ls {self.dir_save}'

        output_rname = subprocess.check_output(command_save, shell=True)
        path_random_name = output_rname.decode().split('\n')
        random_file_name = path_random_name[-2].split('/')[-1]
        return random_file_name

    def clear(self, name):
        '''
        Delete the original files and the intermediate folder
        :param self:
        :param name: file name
        :return:
        '''
        # delete source file
        command_delete_file = f'hdfs dfs -rm {self.dir_in}/{name}'
        subprocess.run(command_delete_file, shell=True)

        # delete tmp dir
        command_delete_spark_save = f'hdfs dfs -rm -r {self.dir_save}'
        subprocess.run(command_delete_spark_save, shell=True)

    def process_file(self, name, spark):
        """
        Form the data processing process for each file
        :param name: file name
        :param spark: SparkSession object for reading the file
        :return:
        """
        try:
            csv_file = f'{self.dir_in}/{name}'
            column_value = name.split('_')[1]

            self.logger.info(f'Обрабатываем файл {self.dir_in}/{name}')
            self.change_file(csv_file, column_value, spark)
            self.logger.info(f'Файл {self.dir_in}/{name} успешно обработан')

            random_file_name = self.get_random_file_name()
            # change file_name and exception .csv in csv.gz
            self.logger.info(f'Перемещаем файл {name} в директорию {self.dir_out}')
            if name.endswith('.csv'):
                command_renaming = f'hdfs dfs -mv {self.dir_save}/{random_file_name} {self.dir_out}/{name.replace(".csv", ".csv.gz")}'
            else:
                command_renaming = f'hdfs dfs -mv {self.dir_save}/{random_file_name} {self.dir_out}/{name}'

            subprocess.run(command_renaming, shell=True)
            self.logger.info(f'Файл {name} успешно перемещен')

            self.logger.info(f'Очищаем временные файлы и исходный файл')
            self.clear(name)
            self.logger.info(f'Очистка произведена успешно')

        except Exception as e:
            self.logger.error(f'Ошибка при обработке файла {name}: {e}', exc_info=True)

    def check_dir(self, directory):
        '''

        :param directory:
        :return:
        '''
        # checking the existence of a directory
        command_check = f'hdfs dfs -test -e {directory}'
        result = subprocess.run(command_check, shell=True)
        folder_exist = result.returncode == '0'

        if not folder_exist:
            command_mkdir = f'hdfs dfs -mkdir -p {directory}'
            subprocess.run(command_mkdir, shell=True)
            self.logger.info(f'Создана папка {directory}')

    def process_run(self):
        """

        :return:
        """
        files_name = self.get_file_names()
        if not files_name:
            self.logger.info('Нет файлов для обработки')
            return

        spark = (
            SparkSession
            .builder
            .master('yarn')
            .appName('HDFS connection')
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            .getOrCreate()
        )

        try:
            self.check_dir(dir_out)
            self.check_dir(dir_save)

            for name in files_name[:5]:
                self.process_file(name, spark)

        except Exception as e:
            self.logger.error(f'Ошибка при обработке файлов {e}', exc_info=True)
        finally:
            spark.stop()
            self.logger.info('Сессия Spark остановлена')


if __name__ == '__main__':
    dir_in = '/raw_data/source_08_rnis/files/telematics'
    dir_out = '/raw_data/source_08_rnis/files/telematics_test'
    dir_save = '/raw_data/source_08_rnis/files/spark_save'

    file_processor = FileProcessor(dir_in, dir_out, dir_save)
    file_processor.process_run()6f93
