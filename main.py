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
    file_processor.process_run()
