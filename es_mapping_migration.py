#!/usr/bin/env python
# coding: utf-8

import requests
import os
import json
import copy
from threading import local

#HOST = "elastic.leapfrog.qa.mm-corp.net"
if 'ES_SOURCE_HOST' not in os.environ:
    raise LookupError("Must define ES_SOURCE_HOST in Marathon/Docker env.")

BASEURL = os.environ['ES_SOURCE_HOST']

if not BASEURL.startswith('http'):
    raise ValueError("ES_SOURCE_HOST must use the http scheme.")

if 'ES_DEST_HOST' not in os.environ:
    raise LookupError("Must define ES_DEST_HOST in Marathon/Docker env.")

DESTURL = os.environ['ES_DEST_HOST']

if not DESTURL.startswith('http'):
    raise ValueError("ES_DEST_HOST must use the http scheme.")

OPTIMIZE_FOR_BULK = False
if 'OPTIMIZE_FOR_BULK' in os.environ and os.environ['OPTIMIZE_FOR_BULK']:
    OPTIMIZE_FOR_BULK = True

class IndexExistsError(Exception):
    """The index is already defined."""
    def __init__(self, value):
        self.value = "IndexExistsError: "+value
    def __str__(self):
        return repr(self.value)

def get_json(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

THREAD_LOCALS = local()
    
def get_index(index, doc):
    THREAD_LOCALS.INDEX = index
    THREAD_LOCALS.DOC = doc
    SEP = "/"
    URL = SEP.join([BASEURL,index,doc,"_mapping"])
    #print(URL)
    mapping_json = get_json(URL)
    
    # Get the settings
    SURL = SEP.join([BASEURL,index,"_settings"])
    #print(SURL)
    settings_json = get_json(SURL)
    
    # Get the aliases
    AURL = SEP.join([BASEURL,index,"_aliases"])
    aliases_json = get_json(AURL)

    mapping_for_doc = mapping_json[index]['mappings'][doc]
    properties = mapping_for_doc['properties']

    settings = settings_json[index]['settings']['index']

    aliases = aliases_json[index]['aliases']
    return (properties, settings, aliases)

def get_es2_indices_doctypes(source_es):
    SEP = "/"
    URL = SEP.join([source_es,"_mappings"])
    all_list_dict = get_json(URL)
    tuples = []
    ignored = []
    if isinstance(all_list_dict, dict):
        for k in all_list_dict:
            if 'mappings' in all_list_dict[k]:
                index_mappings = all_list_dict[k]['mappings']
                if len(index_mappings) == 1:
                    for doctype in index_mappings:
                        #print (f'{k},{doctype}')
                        tuples.append((k, doctype))
                else:
                    #print (f'Ignoring index {k} with > 1 doctype.')
                    for doctype in index_mappings:
                        ignored.append((k, doctype))
                    
    return {
        'validIndexDoctype': tuples,
        'ignored': ignored
    }

# Specify fields to keep as raw string type only (not analyzed)
# Specify per index in Docker/Maratho ENV NOT_ANALYZED_FIELDS_{index-name} as comma-separated names
NOT_ANALYZED_FIELDS_PREFIX = 'NOT_ANALYZED_FIELDS_'

RAW_STRING_2_5 = ({
    'type': 'string',
    'index': 'not_analyzed'
},{
    'type': 'keyword',
    'fields': {}
})

# ES 5.x from 2.x breaking mappings.
CORE_TYPES_2_5 = {
    'string': {
        'type': 'text',
        'fields': {
            'raw': {
                'type': 'keyword',
                'ignore_above': 256
            }
        }
    }
}


#substitute normalizer
LC_NORMALIZER =  {
            'filter': ['lowercase']
}

ANALYZERS_TO_NORMALIZERS={
    'lower_case_sort': LC_NORMALIZER
}

def get_specified_raw_types():
    """Get fields to be mapped as pure raw fields (aka not_analyzed).
    Environment variable name convention is NOT_ANALYZED_FIELDS_{index-name}"""
    env_name = f'{NOT_ANALYZED_FIELDS_PREFIX}{THREAD_LOCALS.INDEX}'
    if env_name in os.environ:
        return os.environ[env_name]
    else:
        return []

# Handlers for different mapping elements will be chained using annotations.

def handleNormalizers(analyzersHandler):
    """Decorates the analyzersHandler.
    Remap analyzers specified in 'analyzers_to_normalizers' to normalizers"""
    def wrapper(*args, **kwargs):
        el, new_mapping, unprocessed_analyzers = analyzersHandler(*args, **kwargs)
        for key in unprocessed_analyzers:
            normalizer = unprocessed_analyzers[key]
            normalizer['type'] = 'keyword'
            normalizer.pop('analyzer')
            normalizer['normalizer']= key
            new_mapping['fields'][key] = normalizer
        return el, new_mapping
    return wrapper

def handleAnalyzers(fieldsHandler):
    """An analyzers decorator around fields"""
    def wrapper(*args, **kwargs):
        el, new_mapping, handleThis = fieldsHandler(*args, **kwargs)
        fields = el
        unprocessed_analyzers = {}
        if handleThis and fields:
            for key in fields:
                if 'analyzer' in fields[key]:
                    if key not in ANALYZERS_TO_NORMALIZERS.keys():
                        analyzer = fields[key]
                        analyzer['type'] = 'text'
                        new_mapping['fields'][key] = analyzer
                    else:
                        unprocessed_analyzers[key]= fields[key]
        return el, new_mapping, unprocessed_analyzers
    return wrapper
            
def handleFields(typeHandler):
    """A fields decorator around types"""
    def wrapper(*args, **kwargs):
        el, new_mapping = typeHandler(*args, **kwargs)
        if 'fields' in el:
            el = el['fields']
            # delegate to analyzerHandler
            return el, new_mapping, True
        else:
            return el, new_mapping, False
    return wrapper

@handleNormalizers
@handleAnalyzers
@handleFields
def handleTypes(el, name, new_mapping=None):
    """ Modular handling of mapping element.
    Apply additional transformations using Decorator pattern."""
    if 'type' in el:
        typeStr = el['type']
        if typeStr in CORE_TYPES_2_5:
            if el == RAW_STRING_2_5[0] or name in get_specified_raw_types():
                print("raw type: ", name)
                new_mapping = copy.deepcopy(RAW_STRING_2_5[1])
            else:
                new_mapping = copy.deepcopy(CORE_TYPES_2_5[typeStr])
    return el, new_mapping
            

def migrate_mapping_element(el, name):
    el, new_mapping = handleTypes(el, name)
    if new_mapping:
        el = new_mapping
    return el


# remap 2 to 5
MAXDEPTH = 20
class MaxDepthError(Exception):
    pass

#def remap(key, props):
#    mapping = props[key]
#    #print(mapping)
#    props[key] = remap_core_types_2_5(mapping)

def recursive_remap(key, props, depth):
    keyref = props[key]
    if depth > MAXDEPTH:
        raise MaxDepthError
    
    if 'properties' in keyref:
        depth += 1
        childref = keyref['properties']
        print(f"recurse {key} at depth {depth}")
        for childkey in childref:
            recursive_remap(childkey, childref, depth)
    else:
        props[key] = migrate_mapping_element(keyref, key)

def remap_settings():
    # https://www.elastic.co/guide/en/elasticsearch/reference/5.5/breaking_50_settings_changes.html
    remapped_settings = []
    REMAP_INDEXSETTINGS_ES5 = {
        'max_clause_count': lambda s: print("Replaced with indices.query.bool.max_clause_count - node-level setting."),
        'max_terms_count': lambda s: print("Unknown: every setting must be a known setting. See breaking changes.")
                }


#properties['EAN']
#settings['analysis']
#aliases


def migrate(properties):
    # just for debugging
    IGNOREDOC = True
    if IGNOREDOC and 'doc' in properties:
        properties.pop('doc',None)
    for key in properties:
        recursive_remap(key, properties, 0)

def process_migrated_settings(path, index, doc, index_def):
    SCHEME = 'http'
    if(path.startswith(SCHEME)):
        return put_mappings_in_dest(path, index, index_def)
    else:
        save_file(path, index, index_def)   

def save_file(path, index, index_def):
    file = path + f'/{index}-mapping.json'
    with open(file, 'w') as outfile:
            json.dump(index_def, outfile) 

def put_mappings_in_dest(path, index, index_def):
    url = f'{path}/{index}'
    index_defined = True
    error = None
    success = None
    if requests.get(url).status_code != 200:
        index_defined = False
        print (f"PUT ting settings into {url}")
        try:
            put_res = requests.put(url, json=index_def)
            if put_res.status_code == 200:
                success = index
            put_res.raise_for_status()
        except Exception as e:
            error = e
    else:
        error=IndexExistsError(url)
        
    return {
        'success': success,
        'error': error
    }

def save_migration(path, index, doc, properties, settings, aliases):
    # write modified settings & mapping to file
    mappings={
          doc:{
              'properties': properties
          }
    }
    index_def = {}
    index_def['settings'] = {}

    if 'analysis' in settings and settings['analysis']:
        settings['analysis']['normalizer'] = ANALYZERS_TO_NORMALIZERS
        index_def['settings']['analysis'] = settings['analysis']

    if OPTIMIZE_FOR_BULK:
        index_def['settings']['index'] = {}
        index_def['settings']['index']['number_of_replicas'] = 0
        index_def['settings']['index']['refresh_interval'] = -1
    
    if aliases:
        index_def['aliases'] = aliases

    index_def['mappings'] = mappings
    if path:
        return process_migrated_settings(path, index, doc, index_def)
    else:
        return index_def

def migrate_indices(source_es, dest_es):
    all_indices = get_es2_indices_doctypes(source_es)
    valid_indices = all_indices['validIndexDoctype']
    result = []
    for (index, doctype) in valid_indices:
        print("Migrating ", index, " and doc ", doctype)
        try:
            (properties, settings, aliases)=get_index(index, doctype)
            migrate(properties)
            res = save_migration(dest_es, index, doctype, properties, settings, aliases)
            result.append(res)
        except Exception:
            pass
    return result

# Main Method begins here
#INDEX = "my_index"
#DOC = "my_index_doctype"
#(properties, settings, aliases) = get_index(INDEX, DOC)
#migrate(properties)
#save_migration('/Users/user/Downloads')
