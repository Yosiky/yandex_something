from aiohttp import web
from json.decoder import JSONDecodeError
from datetime import datetime


routes = web.RouteTableDef()
database = dict()


class ValidationError(ValueError):
    def __init__(self, s):
        super().__init__(s)


def debug_print(*args):
    print(*args)


def bad_request():
    return web.json_response({'message': 'Validation Failed', 'code': 400}, status=400)


def ok_response(value):
    return web.json_response(value, status=200)


def not_found():
    return web.json_response({'message': 'Item not found', 'code': 404}, status=404)


def node_found(node):
    debug_print(f'SENDING: {node}')
    return web.json_response(node, status=200)


pattern = [
    {
        "items": list,
        "updateDate": str
    },
    {
        "id": str,
        "name": str,
        "[parentId]": (str, type(None)),
        "[price]": int,
        "type": str
    },
    {
        "id": str,
        "name": str,
        "[parentId]": (str, type(None)),
        "[price]": int,
        "type": str
    },
    {
        "date": str
    }
]


def json_valid(data, expected):
    for ekey in expected:
        if ekey.startswith('[') and ekey.endswith(']'):
            dkey = ekey[1:-1]
            if dkey not in data:
                continue
        else:
            dkey = ekey
        if dkey not in data:
            raise ValidationError(f'key `{dkey}` not in data')
        if isinstance(expected[ekey], (type, tuple)):
            if not isinstance(data[dkey], expected[ekey]):
                raise ValidationError(f'expected {int} type by key `{dkey}`')
        elif isinstance(expected[ekey], (list, dict)):
            if type(expected[ekey]) != type(data[dkey]):
                raise ValidationError(f'expected {type(expected[ekey])} type by key `{dkey}`')
            json_valid(data[dkey], expected[ekey])
        else:
            raise ValidationError(f'unknown type by value `{dkey}` ({type(expected[ekey])})')


@routes.view('/imports')
class ImportHandler(web.View):
    async def post(self):
        try:
            def update_parents(id, date):
                if id is not None:
                    database[id]['date'] = date
                    if database[id]['parentId'] is not None:
                        update_parents(database[id]['parentId'], date)

            receive_data = await self.request.json()
            debug_print('INFO: get content')
            json_valid(receive_data, pattern[0])
            for item in receive_data['items']:
                if 'type' not in item:
                    raise ValidationError('key `type` not in data')
                elif item['type'] == 'CATEGORY':
                    json_valid(item, pattern[1])
                elif item['type'] == 'OFFER':
                    json_valid(item, pattern[2])
                else:
                    raise ValidationError(f'unknown type `{item[type]}`')
                children = set()
                if item['id'] in database:
                    children = database[item['id']]['children']
                database[item['id']] = item
                database[item['id']]['children'] = children
                if item['parentId'] is not None:
                    database[item['parentId']]['children'].add(item['id'])
                update_parents(item['id'], receive_data['updateDate'])
            return ok_response({})
        except (JSONDecodeError, ValidationError) as exc:
            debug_print('ERROR: bad request:\n', exc)
            return bad_request()


@routes.view('/delete/{id}')
class DeleteHandler(web.View):
    async def delete(self):
        def delete_tree(item):
            if item is None:
                return
            for i in item['children']:
                delete_tree(database[i])
                del database[i]

        id_item = self.request.match_info.get('id')
        debug_print(f'INFO: delete id {id_item}')
        if id_item is None:
            return bad_request()
        elif id_item not in database:
            return not_found()
        if database[id_item]['parentId'] is not None:
            database[database[id_item]]['children'].remove(id_item)
        delete_tree(database[id_item])
        del database[id_item]
        return ok_response({'message': "all deleted"})


@routes.view('/nodes/{id}')
class NodesHandler(web.View):
    async def get(self):
        def create_tree(data):
            data_cpy = dict(data)
            kids = [create_tree(database[i]) for i in data['children']]
            if len(kids) == 0 and data['type'] == 'OFFER':
                kids = None
            data_cpy['children'] = kids
            return data_cpy

        def mean_price(node):
            if node['children'] is None:
                prices = []
            else:
                prices = [mean_price(nd) for nd in node['children']]
            if len(prices) == 0:
                prices_sum, prices_num = 0, 0
            else:
                prices_sum, prices_num = list(zip(*prices))
                prices_sum, prices_num = sum(prices_sum), sum(prices_num)
            if node['type'] == 'OFFER':
                prices_sum += node['price']
                prices_num += 1
            else:
                if prices_num == 0:
                    prices_mean = 0
                else:
                    prices_mean = prices_sum / prices_num
                node['price'] = round(prices_mean)
            return prices_sum, prices_num

        id_item = self.request.match_info.get('id')
        debug_print(f'INFO: get id {id_item}')
        if id_item is None:
            return bad_request()
        if id_item not in database:
            return not_found()
        tree = create_tree(database[id_item])
        mean_price(tree)
        return node_found(tree)


@routes.view('/sales/')
class SalesHandler(web.View):
    async def get(self):
        receive_data = await self.request.json()
        debug_print(f'INFO: get date {receive_data}')
        for item in database:
            print(item['date'])
        return ok_response({})


app = web.Application()
app.add_routes(routes)


def main():
    web.run_app(app, port=8080, host='0.0.0.0')


if __name__ == '__main__':
    main()