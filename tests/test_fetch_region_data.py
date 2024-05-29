import unittest
from src.region.fetch_region_data import gather_region_data

class TestFetchRegionData(unittest.TestCase):

    def test_gather_region_data(self):
        API_HOST = "m.land.naver.com"
        API_PATH = "/map/getRegionList"
        HEADERS = {'Cookie': 'NID_SES=123'}
        WORK_DIR = '/Users/jodongik/workspace/prj-naver-api/data/resource'
        TIME_SLEEP_SECONDS = 2.8122

        df = gather_region_data(API_HOST, API_PATH, HEADERS, WORK_DIR, TIME_SLEEP_SECONDS)
        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

if __name__ == '__main__':
    unittest.main()
