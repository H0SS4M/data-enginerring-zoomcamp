if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import re

@transformer
def transform(df, *args, **kwargs):

    df = df[(df['passenger_count'] != 0) & (df['trip_distance'] != 0)]
    
    df.rename(columns = {'VendorID':'vendor_id', 'RatecodeID':'ratecode_id', 
        'PULocationID': 'pu_location_id', 'DOLocationID': 'do_location_id'}, inplace = True)

    df['lpep_pickup_date'] = df.lpep_pickup_datetime.dt.date

    return df


@test
def test_output(df, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
    assert 'vendor_id' in df.columns
    assert (df[df['passenger_count'] == 0]['passenger_count'].sum() == 0)
    assert (df[df['trip_distance'] == 0]['trip_distance'].sum() == 0)
