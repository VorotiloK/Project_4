Необходимо написать скрипт, который извлекает информацию с веб-страниц
Confluence и сохраняет данные в определенной директории. Скрипт работает с
API Confluence для доступа к страницам, извлекает текстовую информацию
из HTML-контента страниц и сохраняет эту информацию в файлы с именами,
основанными на названиях проектов и версий.
Для решения использовал:
- os для взаимодействия с операционной системой, создания директорий и файлов.
- atlassian.confluence для работы с API Confluence и получения данных с
веб-страниц.
- BeautifulSoup для парсинга HTML-контента страниц Confluence и извлечения
информации.
- re для работы с регулярными выражениями (например, для очистки названий
проектов от недопустимых символов