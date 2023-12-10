import pandas as pd


def generate_car_matrix(df)->pd.DataFrame:
    """
    Creates a DataFrame  for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
    df = pd.read_csv(df)

    
    matrix = df.pivot(index='id_1', columns='id_2', values='car').fillna(0)

    
    for i in range(min(len(matrix.index), len(matrix.columns))):
        matrix.iloc[i, i] = 0

    return matrix

    #return df


def get_type_count(df)->dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    # Write your logic here
    car_type=[]
    for i in df['car']:
        if i<=15:
            car_type.append('low')
        elif i>15 and i<=25:
            car_type.append('medium')
        else:
            car_type.append('high')
        
    df['car_type']=car_type
    d=df['car_type'].value_counts().to_dict()

    type_count = dict(sorted(d.items()))

    return type_count


def get_bus_indexes(df)->list:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
    # Write your logic here
    #d = pd.read_csv(df)
    
    mean_bus = df['bus'].mean()  # Calculate the mean of the 'bus' column
    threshold = 2 * mean_bus  # Define the threshold
    lst=[]
    # Filter indices where 'bus' values are greater than twice the mean
    for i,j in enumerate(df['bus']):
      if j>threshold:
        lst.append(i)

    
    return sorted(lst)


def filter_routes(df)->list:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    #df = pd.read_csv(df)
    l=[]
    for i in range(len(df['truck'])):
      if df['truck'][i]>df['truck'].mean():
        l.append(df['route'][i])
        l.sort()
    return l


def multiply_matrix(matrix)->pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
    for col in matrix.columns:
        for index, value in matrix[col].items():
            if value > 20:
                matrix.at[index, col] = round(value * 0.75, 1)
            else:
                matrix.at[index, col] = round(value * 1.25, 1)
    
    return matrix


def time_check(df)->pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """
    try:
        df['start'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], format='%A %H:%M:%S', errors='raise')
        df['end'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'], format='%A %H:%M:%S', errors='raise')
    except ValueError as e:
        print(f"ValueError: {e}")
        return pd.Series(dtype=bool)

    # Group data by (id, id_2) and check for completeness of timestamps
    completeness = df.groupby(['id', 'id_2']).apply(lambda x: x['end'].max() - x['start'].min() != pd.Timedelta(days=7))
    
    return completeness
