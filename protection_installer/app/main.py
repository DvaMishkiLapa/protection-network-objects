import os
from datetime import datetime, timedelta

from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from flask_sqlalchemy import SQLAlchemy
from jsonschema import ValidationError, validate

from logger import create_logger

logger = create_logger('log/flask.log', 'flask')

app = Flask(__name__)
api = Api(app)

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://{}:{}@{}/{}'.format(
    os.getenv('MYSQL_USER', 'flask'),
    os.getenv('MYSQL_PASSWORD', ''),
    os.getenv('MYSQL_HOST', 'mysql'),
    os.getenv('MYSQL_DATABASE', 'flask')
)
logger.debug(f'Connection to MySQL: {app.config["SQLALCHEMY_DATABASE_URI"]}')
db = SQLAlchemy(app)


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


class ProtectedObjects(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    defense = db.Column(db.Integer, nullable=False)
    start_protect = db.Column(db.DateTime(), nullable=False)
    end_protect = db.Column(db.DateTime(), nullable=False)

    def __repr__(self):
        return '<ProtectedObjects %r>' % self.id


@app.before_first_request
def create_tables():
    db.create_all()


class Protect(Resource):
    def __init__(self):
        self.protected_objects = {}
        self.__db_to_ram()

    def post(self):
        try:
            validate(request.json, schema)
        except ValidationError as e:
            logger.error(f'ValidationError json_path: {e.json_path}, error: {e.schema}')
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
        self.__del_old_protect()
        now_datatime = datetime.now()
        end_protect_datatime = now_datatime + timedelta(seconds=content['TTL'])
        new_info = {
            'id': content['ID'],
            'defense': content['DEFENSE'],
            'start_protect': now_datatime,
            'end_protect': end_protect_datatime
        }
        check_row = ProtectedObjects.query.filter_by(id=content['ID'])
        if check_row.scalar() is None:
            db.session.add(ProtectedObjects(**new_info))
            db.session.commit()
        else:
            logger.debug(f'Duplicate ID {content["ID"]}, update')
            check_row.update(new_info)
            db.session.commit()
            self.protected_objects.update({
                content['ID']: {
                    'DEFENSE': content['DEFENSE'],
                    'START_PROTECTION': now_datatime.strftime('%m-%d-%Y, %H:%M:%S'),
                    'END_PROTECTION': end_protect_datatime.strftime('%m-%d-%Y, %H:%M:%S')
                }
            })
        return {'ID': content['ID'], 'result': 'ok'}, 200

    def __cmd_pull(self):
        logger.debug(f'PULL')
        self.__del_old_protect()
        self.protected_objects = {k: self.protected_objects[k] for k in sorted(self.protected_objects)}
        return self.protected_objects, 200

    def __del_old_protect(self):
        logger.debug(f'Del old protect objects')
        objects = ProtectedObjects.query.all()
        now_datatime = datetime.now()
        for ob in objects:
            if ob.end_protect < now_datatime:
                ProtectedObjects.query.filter_by(id=ob.id).delete()
                self.protected_objects.pop(ob.id, None)
        db.session.commit()

    def __db_to_ram(self):
        objects = ProtectedObjects.query.all()
        self.protected_objects.clear()
        for ob in objects:
            self.protected_objects.update({
                ob.id: {
                    'DEFENSE': ob.defense,
                    'START_PROTECTION': ob.start_protect.strftime('%m-%d-%Y, %H:%M:%S'),
                    'END_PROTECTION': ob.end_protect.strftime('%m-%d-%Y, %H:%M:%S')
                }
            })


api.add_resource(Protect, '/')


if __name__ == '__main__':
    logger.info('Flask service start')
    app.run(host='0.0.0.0', port=8000, debug=True)
