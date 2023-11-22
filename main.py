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