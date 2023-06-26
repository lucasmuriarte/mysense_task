from pyflink.table import DataTypes
from pyflink.table.udf import udf
import pytz
from datetime import datetime

@udf(result_type=DataTypes.BOOLEAN())
def row_selection(cid, heartrates):
    """
    Select the rows fot a particular cid if the heartrates has data
    """
    if cid != '5f2cc245-9c8d-4c40-b764-9210d0e2ffb1':
        return False
    if heartrates and len(heartrates) > 0:
        return True
    return False


class RowTimeExpansion:
    """
    Expands the row time data from the heartrates can be used with a flat_map
    function
    """
    def __init__(self, time_zone):
        self.time_zone = time_zone
    
    def __call__(self, cid, heartrates):
        for row in heartrates:
            yield cid, row.type, row.heartrate, self.format_time(row.ts)
            
    def format_time(self, time):

        date = datetime.fromtimestamp(time / 1e3)
        desired_timezone = pytz.timezone(self.time_zone)
        time_update = date.astimezone(desired_timezone)
        formatted_datetime = time_update.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_datetime

