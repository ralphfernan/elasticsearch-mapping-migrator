from flask import Flask, request
from flask_restplus import Resource, Api
from es_mapping_migration import migrate_indices, get_index, migrate, save_migration, BASEURL, DESTURL

app = Flask(__name__)
api = Api(app, version='1.0', title='Elastic 2 to 6 Migration API',
    description='Generates mappings, settings, aliases for defining a new index in the new cluster, prior to migrating data. PUTs /myindex with migrated json.',
)

@api.route('/migrate-one/<string:index>/<string:doctype>')
@api.doc(description=f'Migrate given index from source ES {BASEURL} to dest ES {DESTURL}',
	params={'index': 'The source index name.','doctype': 'The doctype for the index.'})
class ESMigrateGiven(Resource):
	def put(self, index, doctype):
		(properties, settings, aliases)=get_index(index, doctype)
		migrate(properties)
		result = save_migration(DESTURL, index, doctype, properties, settings, aliases)
		return result['success'] or str(result['error'])

@api.route('/migrate-indices')
@api.doc(description=f'Migrate valid indices (with defined mapping and single doctype) from source ES {BASEURL} to dest ES {DESTURL}')
class ESMigrateIndices(Resource):
	def put(self):
		result = migrate_indices(BASEURL, DESTURL)
		print(len(result))
		return [item['success'] or str(item['error']) for item in result]

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)