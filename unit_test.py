import json

import pytest

from tasks import DataCalculationTask, DataAggregationTask, DataAnalyzingTask


@pytest.fixture()
def city_data():
    with open("examples/response.json", "r") as f:
        return json.load(f)


@pytest.fixture()
def calculate_data():
    return dict(
        city="Moscow",
        days=[
            {
                "date": "2022-05-18",
                "weather": {"average_temp": 14, "no_rain": 11},
            },
            {
                "date": "2022-05-19",
                "weather": {"average_temp": 14, "no_rain": 5},
            },
            {
                "date": "2022-05-20",
                "weather": {"average_temp": 14, "no_rain": 11},
            },
            {
                "date": "2022-05-21",
                "weather": {"average_temp": None, "no_rain": 0},
            },
            {
                "date": "2022-05-22",
                "weather": {"average_temp": None, "no_rain": 0}},
        ],
        average_temp=14,
        no_rain=9,
    )


@pytest.fixture()
def items_data():
    return [
        {"average_temp": 10, "no_rain": 1},
        {"average_temp": 11, "no_rain": 1},
        {"average_temp": 10, "no_rain": 2},
    ]


@pytest.fixture()
def analysis_data():
    return [
        {"city": "1", "average_temp": 10, "no_rain": 1, "rating": 4},
        {"city": "2", "average_temp": 11, "no_rain": 1, "rating": 1},
        {"city": "3", "average_temp": 10, "no_rain": 2, "rating": 3},
        {"city": "4", "average_temp": 11, "no_rain": 1, "rating": 2},
    ]


def test_calculate(city_data, calculate_data):
    result = DataCalculationTask.calculate(city_data)

    assert result == calculate_data


def test_set_rating(items_data):
    DataAggregationTask.set_rating(items_data)

    assert items_data[0]["rating"] == 3
    assert items_data[1]["rating"] == 1
    assert items_data[2]["rating"] == 2


def test_get_best_cities(analysis_data):
    best_city = DataAnalyzingTask.get_best_cities(analysis_data)

    assert best_city == ["2", "4"]
