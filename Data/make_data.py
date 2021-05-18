# -*- coding: utf-8 -*-

"""
Collection of methods to make data
"""

# Built-in
from datetime import date
from typing import Union, List

# Other
from geopy import distance
import pandas as pd
import requests

# API URL
weather_api_url = "https://api.weather.gc.ca"


def scrape_weather_stations(provinces: Union[str, List[str]]) -> pd.DataFrame:
    """
    Scrape all weather stations from the GC Canada Weather API

    Args:
        province (Union[str, List[str]]): Either a single province or list of provinces to get weather stations from

    Returns:
        pd.DataFrame: Returns a dataframe containing data on all weather stations
    """
    if type(provinces) == str:
        provinces = [provinces]

    query_url = weather_api_url + "/collections/climate-stations/items"
    all_weather_stations = pd.DataFrame()

    for province in provinces:
        params = {"f": "json", "ENG_PROV_NAME": province, "startindex": 1}

        # Repeatedly get weather stations in chunks of 500
        while True:
            response = requests.get(query_url, params=params)
            weather_stations = [
                row["properties"] for row in response.json()["features"]
            ]
            weather_stations = pd.DataFrame(weather_stations)
            all_weather_stations = pd.concat([all_weather_stations, weather_stations])

            # If 500 weather stations returned then start at next index
            if weather_stations.shape[0] == 500:
                params["startindex"] += 500
            else:
                break

    all_weather_stations.reset_index(inplace=True, drop=True)

    # Fix some column types
    all_weather_stations = all_weather_stations.assign(
        LONGITUDE=lambda x: x["LONGITUDE"] / 10e6,
        LATITUDE=lambda x: x["LATITUDE"] / 10e6,
        DLY_FIRST_DATE=lambda x: pd.to_datetime(x["DLY_FIRST_DATE"]),
        DLY_LAST_DATE=lambda x: pd.to_datetime(x["DLY_LAST_DATE"]),
        FIRST_DATE=lambda x: pd.to_datetime(x["FIRST_DATE"]),
        LAST_DATE=lambda x: pd.to_datetime(x["LAST_DATE"]),
    )

    return all_weather_stations


def closest_weather_station(
    latitude: float, longitude: float, date_: date, weather_stations: pd.DataFrame
) -> dict:
    """
    Gets the closest weather station to the given location and date

    Args:
        latitude (float): Latitude
        longitude (float): Longitude
        date_ (date): Date
        weather_stations (pd.DataFrame): Dataframe of weather stations to consider

    Returns:
        dict: Dictionary of properties of closest weather station
    """
    # Filter by weather stations that include the given date
    weather_stations = weather_stations.query(
        "FIRST_DATE < @date_ and LAST_DATE > @date_"
    ).reset_index(drop=True)

    # Calculate pairwise distance between given location and each weather station
    distances = weather_stations.apply(
        lambda x: distance.distance(
            (latitude, longitude), (x["LATITUDE"], x["LONGITUDE"])
        ).km,
        axis=1,
    )
    weather_stations = weather_stations.assign(DISTANCE_WEATHER_STATION_KM=distances)

    # Get closest weather station
    # weather_stations.sort_values(by="DISTANCE_WEATHER_STATION_KM", inplace=True)
    # closest_station = weather_stations.iloc[0].to_dict()

    closest_station = weather_stations.iloc[
        weather_stations["DISTANCE_WEATHER_STATION_KM"].idxmin()
    ].to_dict()

    return closest_station


def weather_data(
    latitude: float, longitude: float, date_: date, weather_stations: pd.DataFrame
):
    """
    Get weather data for given location and date using the closest weather station

    Args:
        latitude (float): Latitude
        longitude (float): Longitude
        date_ (date): Date
        weather_stations (pd.DataFrame): DataFrame of weather stations to consider

    Returns:
        pd.DataFrame: DataFrame containing weather data
    """
    # Get closest weather station to the given location to use to get weather from
    closest_station = closest_weather_station(
        latitude, longitude, date_, weather_stations
    )
    query_url = weather_api_url + "/collections/climate-daily/items"
    params = {
        "f": "json",
        "CLIMATE_IDENTIFIER": closest_station["CLIMATE_IDENTIFIER"],
        "LOCAL_DAY": date_.day,
        "LOCAL_MONTH": date_.month,
        "LOCAL_YEAR": date_.year,
    }
    response = requests.get(query_url, params=params)
    weather_list = [row["properties"] for row in response.json()["features"]]

    # If empty then there was no information for that date and location
    if len(weather_list) == 0:
        weather = pd.DataFrame(closest_station, index=[0])[
            [
                "STATION_NAME",
                "STN_ID",
                "CLIMATE_IDENTIFIER",
                "DISTANCE_WEATHER_STATION_KM",
            ]
        ]
        return weather
    else:
        weather = pd.DataFrame(weather_list).drop(
            [
                "LOCAL_DATE",
                "LOCAL_DAY",
                "LOCAL_MONTH",
                "LOCAL_YEAR",
                "ID",
                "PROVINCE_CODE",
            ],
            axis=1,
        )

        # Add distance to weather station and station id
        weather.insert(
            loc=1,
            column="DISTANCE_WEATHER_STATION_KM",
            value=closest_station["DISTANCE_WEATHER_STATION_KM"],
        )

        weather.insert(loc=1, column="STN_ID", value=closest_station["STN_ID"])

    return weather
