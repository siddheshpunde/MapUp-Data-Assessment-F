import pandas as pd
import numpy as np
import itertools
from datetime import time

def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    

    
    distances = {}
    for _, row in df.iterrows():
        distances[(row['id_start'], row['id_end'])] = row['distance']
        distances[(row['id_end'], row['id_start'])] = row['distance']

    
    unique_ids = sorted(set(df['id_start']) | set(df['id_end']))

    
    num_ids = len(unique_ids)
    max_distance = float('inf')
    distance_matrix = np.full((num_ids, num_ids), max_distance)

    
    for i in range(num_ids):
        for j in range(num_ids):
            if i == j:
                distance_matrix[i][j] = 0
            else:
                id_i = unique_ids[i]
                id_j = unique_ids[j]

                if (id_i, id_j) in distances:
                    distance_matrix[i][j] = distances[(id_i, id_j)]
                elif (id_j, id_i) in distances:
                    distance_matrix[i][j] = distances[(id_j, id_i)]

    
    for k in range(num_ids):
        for i in range(num_ids):
            for j in range(num_ids):
                if distance_matrix[i][k] + distance_matrix[k][j] < distance_matrix[i][j]:
                    distance_matrix[i][j] = distance_matrix[i][k] + distance_matrix[k][j]

    
    distance_df = pd.DataFrame(distance_matrix, columns=unique_ids, index=unique_ids)

    return distance_df


def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    unique_ids = df.columns
    
    
    combinations = list(itertools.product(unique_ids, repeat=2))
    
    
    combinations = [(start, end) for start, end in combinations if start != end]
    
    
    unrolled_distances = []
    
    # Fill the list with distance values from the distance matrix
    for start, end in combinations:
        distance = df.at[start, end]
        unrolled_distances.append({'id_start': start, 'id_end': end, 'distance': distance})
    
    # Create a DataFrame from the list of distances
    unrolled_distances_df = pd.DataFrame(unrolled_distances)
    
    return unrolled_distances_df


def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    average_distance = df[df['id_start'] == reference_id]['distance'].mean()

    
    lower_threshold = average_distance * 0.9
    upper_threshold = average_distance * 1.1

    
    within_threshold = df[(df['id_start'] != reference_id) & 
                                 (df['distance'] >= lower_threshold) & 
                                 (df['distance'] <= upper_threshold)]['id_start']

   
    return sorted(within_threshold.unique())


def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    rate_coefficients = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

    
    for vehicle, coefficient in rate_coefficients.items():
        df[vehicle] = df['distance'] * coefficient

    return df


def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    weekday_discounts = {
        (time(0, 0), time(10, 0)): 0.8,
        (time(10, 0), time(18, 0)): 1.2,
        (time(18, 0), time(23, 59, 59)): 0.8
    }
    weekend_discount = 0.7

    # Extract day and time information from start and end columns
    df['start_day'] = pd.to_datetime(df['start']).dt.day_name()
    df['end_day'] = pd.to_datetime(df['end']).dt.day_name()
    df['start_time'] = pd.to_datetime(df['start']).dt.time
    df['end_time'] = pd.to_datetime(df['end']).dt.time

    # Apply discount factors to vehicle columns based on time intervals
    for idx, row in df.iterrows():
        if row['start_day'] in ['Saturday', 'Sunday']:
            discount_factor = weekend_discount
        else:
            for time_range, discount in weekday_discounts.items():
                if time_range[0] <= row['start_time'] <= time_range[1]:
                    discount_factor = discount
                    break

        for vehicle in ['moto', 'car', 'rv', 'bus', 'truck']:
            df.at[idx, vehicle] *= discount_factor

    return df
