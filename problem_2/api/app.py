import tornado.ioloop
import tornado.web
import pymongo
from tornado import escape
import pprint

""" Services """


class ObjectService:
    parameter_types = {
        'sex_limited': int,
        'sex_limited_not': int,
        'city_name': str,
        'phone_number': str,
        'is_owner': int,
        'landlord_last_name': str,
        'landlord_sex': int,
        'limit': int
    }
    @staticmethod
    def get_objects(
            mongo_collection,
            sex_limited=None,
            sex_limited_not=None,
            city_name=None,
            phone_number=None,
            is_owner=None,
            landlord_last_name=None,
            landlord_sex=None,
            limit=100
        ):
        """
        :param mongo_collection:
        :param sex_limited: both=0, male=1, female=2 ; type: int
        :param sex_limited_not: both=0, male=1, female=2, override sex_limited parameter ; type: int
        :param city_name: type: string
        :param phone_number: type: string
        :param is_owner: owner=1, other=0 ; type: int
        :param landlord_last_name: type: string
        :param landlord_sex: male=1, female=2 ; type: int
        :param limit: max return objects, max=500 type: int
        :return:
        """
        query = {}
        if sex_limited is not None:
            query['sex_limited'] = sex_limited
        if sex_limited_not is not None:
            query['sex_limited'] = {'$ne': sex_limited_not}
        if city_name is not None:
            query['city_name'] = city_name
        if phone_number is not None:
            query['phone_number'] = phone_number
        if is_owner is not None:
            if is_owner == 1:
                query['landlord_type'] = '屋主'
            else:
                query['landlord_type'] = {'$ne': '屋主'}
        if landlord_last_name is not None:
            query['landlord'] = {'$regex': landlord_last_name}
        if landlord_sex is not None:
            query['landlord_sex'] = landlord_sex
        if limit > 500:
            limit = 500
        cursor = mongo_collection.find(query,  {'_id': False, 'fetch_datetime': False}).limit(limit)
        return [obj for obj in cursor]

    @staticmethod
    def parse_parameter(request_handler):
        result = {}
        for parameter_name, parameter_type in ObjectService.parameter_types.items():
            query = request_handler.get_argument(parameter_name, None)
            if query is not None:
                result[parameter_name] = parameter_type(query)
        return result


""" API handlers """


class ObjectHandler(tornado.web.RequestHandler):
    mongo_db_name = 'rent591db'
    collection_name = 'objects'

    def initialize(self, mongo_client):
        self.mongo_client = mongo_client

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def get(self):
        # pprint.pprint(self.request.arguments)
        result = {
            'parameter': {},
            'data': [],
            'status': 0,
            'message': 'query success.'
        }

        # handle parameters
        try:
            parsed_parameter = ObjectService.parse_parameter(self)
        except Exception as e:
            result['status'] = 1
            result['message'] = 'parse parameter failed. >>' + str(e)
            output = escape.json_encode(result)
            self.write(output)
            self.finish()
            return
        result['parameter'] = parsed_parameter
        # pprint.pprint(parsed_parameter)

        # query mongodb
        collection = self.mongo_client[self.mongo_db_name][self.collection_name]
        try:
            query_result = ObjectService.get_objects(collection, **parsed_parameter)
            result['data'] = query_result
        except Exception as e:
            result['status'] = 1
            result['message'] = 'query failed.'

        output = escape.json_encode(result)
        self.write(output)
        # self.write("Hello, world")


def make_app():
    mongo_host = '127.0.0.1'
    mongo_port = 27017
    mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
    return tornado.web.Application([
        (r"/object", ObjectHandler, {'mongo_client': mongo_client}),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()