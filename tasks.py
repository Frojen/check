import json
import logging
import multiprocessing
from multiprocessing import connection, Queue

from api_client import YandexWeatherAPI

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

CLEAR_DAY = ("clear", "partly-cloud", "cloudy", "overcast")


class DataFetchingTask:
    def __init__(self, source: YandexWeatherAPI) -> None:
        self.source = source

    def get_data(self, city_name: str) -> dict:
        resp = self.source.get_forecasting(city_name)
        logger.info(f"Received data about {city_name}")
        return resp


class DataCalculationTask:
    def __init__(self, queue: Queue):
        self.queue = queue

    @staticmethod
    def calculate(data: dict) -> dict:
        result = {
            "city": data["geo_object"]["locality"]["name"],
            "days": [],
        }
        count_temp_days = len_days = no_rain_days = len_no_rain_days = 0
        try:
            for day in data["forecasts"]:
                count_temp_hours = len_hours = no_rain_hours = 0
                for hour in day["hours"]:
                    int_hour = int(hour["hour"])
                    if 9 <= int_hour <= 19:
                        count_temp_hours += int_hour
                        len_hours += 1
                        no_rain_hours += 1 if hour["condition"] in CLEAR_DAY else 0
                average_temp_day = (
                    (count_temp_hours // len_hours) if len_hours else None
                )
                result["days"].append(
                    {
                        "date": day["date"],
                        "weather": {
                            "average_temp": average_temp_day,
                            "no_rain": no_rain_hours,
                        },
                    }
                )
                if average_temp_day:
                    count_temp_days += average_temp_day
                    len_days += 1
                    no_rain_days += no_rain_hours
                    len_no_rain_days += 1
            result["average_temp"] = (count_temp_days // len_days) if len_days else None
            result["no_rain"] = (
                (no_rain_days // len_no_rain_days) if len_no_rain_days else None
            )
        except AttributeError as e:
            logging.warning(f"Error parse weather data! Error {e}")
            raise AttributeError
        logger.info(f"Calculated data about {result['city']}")
        return result

    def callback(self, result: dict) -> None:
        self.queue.put(result)


class DataAggregationTask(multiprocessing.Process):
    def __init__(self, queue: Queue, pipe_in: connection.Connection):
        multiprocessing.Process.__init__(self)
        self.queue = queue
        self.pipe_in = pipe_in

    def run(self):
        items = []
        logger.info("Run aggregation task")
        while not self.queue.empty():
            items.append(self.queue.get(timeout=10))
        logger.info("Received data about cities")

        self.set_rating(items)
        logger.info("Calculated rating")

        with open("result.json", "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
        logger.info("Saved data")

        self.pipe_in.send(items)

    @staticmethod
    def set_rating(items):
        for rating, item in enumerate(
            sorted(
                items, key=lambda x: (x["average_temp"], x["no_rain"]), reverse=True
            ),
            start=1,
        ):
            item["rating"] = rating


class DataAnalyzingTask:
    def __init__(self, pipe_out: connection.Connection):
        self.pipe_out = pipe_out

    def analysis(self) -> None:
        items = self.pipe_out.recv()
        logger.info("Received data from pipe for analysis")
        best_cities = self.get_best_cities(items)

        print(
            f"Наиболее благоприятный(е) город(а) для поездки: {', '.join(best_cities)}"
        )

    @staticmethod
    def get_best_cities(items: list) -> list:
        best_cities = []
        best, *other = sorted(items, key=lambda x: x["rating"])
        best_cities.append(best["city"])
        for item in other:
            if (
                best["average_temp"] == item["average_temp"]
                and best["no_rain"] == item["no_rain"]
            ):
                best_cities.append(item["city"])
            else:
                break
        return best_cities
