from datetime import datetime, timedelta

from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from jsonschema import ValidationError, validate

from logger import create_logger

logger = create_logger('log/flask.log', 'flask')

app = Flask(__name__)
api = Api(app)


schema = {
    'properties': {
        'CMD': {'type': 'string'},
        'ID': {'type': 'integer'},
        'DEFENSE': {
            'type': 'integer',
            'minimum': 1,
            'maximum': 3
        },
        'TTL': {
            'type': 'integer',
            'minimum': 1,
            'maximum': 3600
        }
    },
    'required': ['CMD'],
    'oneOf': [
        {'properties': {'CMD': {'const': 'SET'}}},
        {'properties': {'CMD': {'const': 'PULL'}}}
    ],
    'if': {
        'properties': {'CMD': {'const': 'SET'}}
    },
    'then': {
        'required': ['CMD', 'ID', 'DEFENSE', 'TTL']
    }
}

objects = {}


class Protect(Resource):
    def get(self):
        try:
            validate(request.json, schema)
        except ValidationError as e:
            logger.error(
                f'ValidationError json_path: {e.json_path}, error: {e.schema}')
            return {'json_path': e.json_path, 'error': e.schema}, 400
        content = request.json
        logger.debug(f'Request: {content}')
        if content['CMD'] == 'SET':
            return self.__cmd_set(content)
        elif content['CMD'] == 'PULL':
            return self.__cmd_pull()
        return jsonify(content)

    def __cmd_set(self, content):
        logger.debug(f'SET ID: {content["ID"]}, DEFENSE: {content["DEFENSE"]}, TTL: {content["TTL"]}')
        now_datatime = datetime.now()
        end_protect_datatime = now_datatime + timedelta(seconds=content['TTL'])
        objects.update({
            content['ID']: {
                'DEFENSE': content['DEFENSE'],
                'START_PROTECTION': now_datatime.strftime('%m-%d-%Y, %H:%M:%S'),
                'END_PROTECTION': end_protect_datatime.strftime('%m-%d-%Y, %H:%M:%S')
            }
        })
        return {'ID': content['ID'], 'result': 'ok'}, 200

    def __cmd_pull(self):
        logger.debug(f'PULL')
        return objects, 200


api.add_resource(Protect, '/')


if __name__ == '__main__':
    logger.info('Flask service start')
    app.run(host='0.0.0.0', port=8000, debug=True)
