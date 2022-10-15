import logging
from multiprocessing import Pool, Pipe, Queue
from concurrent.futures import ThreadPoolExecutor

from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    pipe_in, pipe_out = Pipe()
    queue = Queue()
    data_fetching_task = DataFetchingTask(YandexWeatherAPI())
    data_calculation_task = DataCalculationTask(queue)
    data_aggregation_task = DataAggregationTask(queue, pipe_in)
    data_analyzing_task = DataAnalyzingTask(pipe_out)
    logger.info("Starting collection data")
    with ThreadPoolExecutor() as thread:
        cities_data = thread.map(data_fetching_task.get_data, CITIES)
    logger.info("End collection data")
    logger.info("Starting calculate")
    with Pool() as pool:
        for city_data in cities_data:
            pool.apply_async(
                data_calculation_task.calculate,
                args=(city_data,),
                callback=data_calculation_task.callback,
            )
        logger.info("Starting aggregation")
        data_aggregation_task.start()
        pool.close()
        pool.join()
        logger.info("End calculate")
        data_aggregation_task.join()
        logger.info("End aggregation")
    logger.info("Starting analysis")
    data_analyzing_task.analysis()
    logger.info("End analysis")


if __name__ == "__main__":
    forecast_weather()
