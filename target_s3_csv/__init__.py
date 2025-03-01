#!/usr/bin/env python3

import argparse
import io
import json
import os
import sys
import tempfile
import singer

from singer.messages import RecordMessage
from datetime import datetime
from jsonschema import Draft7Validator, FormatChecker

from target_s3_csv import s3
from target_s3_csv import utils
from target_s3_csv import errors
from target_s3_csv.file_handlers import FileHandler, ParquetFileHandler, CSVFileHandler


LOGGER = singer.get_logger('target_s3_csv')
DEFAULT_BATCH_SIZE = 100_000 

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

class TargetS3Parquet:

    def __init__(self, config, s3_client):
        self.config = config
        self.s3_client = s3_client
        self.schemas = {}
        self.validators = {}
        self.filenames = {}
        self.headers = {}
        self.file_handler = CSVFileHandler(self)


    @property
    def temp_dir(self):
        temp_dir = os.path.expanduser(self.config.get('temp_dir', tempfile.gettempdir()))
        if temp_dir:
            os.makedirs(temp_dir, exist_ok=True)
        return temp_dir

    def validate_message(self, message):
        stream_name = message["stream"]
        float_to_decimal_record = utils.float_to_decimal(message['record'])
        try:
            self.validators[stream_name].validate(float_to_decimal_record)
        except Exception as e:
            if type(e).__name__ == "InvalidOperation":
                LOGGER.error(errors.VALIDATION_ERROR)
                raise e

    def get_filename(self, message):
        stream_name = message['stream']
        
        if stream_name in self.filenames:
            filename = self.filenames[stream_name]['filename']
            if os.path.exists(filename):
                return filename

        now = datetime.now().strftime('%Y%m%dT%H%M%S.%f')[:-3]
        filename = os.path.expanduser(os.path.join(self.temp_dir, stream_name + '-' + now + self.file_handler.suffix))

        self.filenames[stream_name] = {
            'filename': filename,
            'target_key': utils.get_target_key(
                message=message,
                prefix=self.config.get('s3_key_prefix', ''),
                timestamp=now,
                naming_convention=self.config.get('naming_convention'),
                format=self.file_handler.suffix
                )
        }
        return filename            
        
        
    def process_message_record(self, message):
        stream_name = message['stream']
        if stream_name not in self.schemas:
            raise Exception(errors.SCHEMA_ERROR.format(stream_name))
        self.validate_message(message)
        record_to_load = message['record']
        if self.config.get('add_metadata_columns'):
            record_to_load = utils.add_metadata_values_to_record(message, {}, self._sdc_batched_at)
        else:
            record_to_load = utils.remove_metadata_values_from_record(message)
        filename = self.get_filename(message)
        flattened_record = utils.flatten_record(record_to_load)
        self.file_handler.write_record_to_file(stream_name, filename, flattened_record)


    def persist_messages(self, messages):
        state = None
        key_properties = {}
        row_count = 0
        total_batches = 0
        self._sdc_batched_at = datetime.now().isoformat()
        for message in messages:
            try:
                parsed_message: dict = singer.parse_message(message).asdict()
            except json.decoder.JSONDecodeError:
                LOGGER.error("Unable to parse:\n{}".format(message))
                raise
            message_type = parsed_message['type']
            if message_type == 'RECORD':
                self.process_message_record(parsed_message)
                row_count += 1
            elif message_type == 'STATE':
                LOGGER.debug('Setting state to {}'.format(parsed_message['value']))
                state = parsed_message['value']

            elif message_type == 'SCHEMA':
                stream_name = parsed_message['stream']
                self.schemas[stream_name] = parsed_message['schema']

                if self.config.get('add_metadata_columns'):
                    self.schemas[stream_name] = utils.add_metadata_columns_to_schema(parsed_message)

                schema = utils.float_to_decimal(parsed_message['schema'])
                self.validators[stream_name] = Draft7Validator(schema, format_checker=FormatChecker())
                key_properties[stream_name] = parsed_message['key_properties']
            elif message_type == 'ACTIVATE_VERSION':
                LOGGER.debug('ACTIVATE_VERSION message')
            else:
                LOGGER.warning("Unknown message type {} in message {}".format(parsed_message['type'], parsed_message))

            if row_count == self.config.get("default_batch_size", DEFAULT_BATCH_SIZE):
                s3.upload_files(
                    iter(self.filenames.values()), 
                    self.s3_client, 
                    self.config['s3_bucket'], 
                    self.config.get("compression"),
                    self.config.get('encryption_type'), self.config.get('encryption_key'), self.config.get('format')
                    )
                if state is not None:
                    emit_state(state)
                row_count = 0
                total_batches += 1
                LOGGER.info(f"total batches is {total_batches}")
                self._sdc_batched_at = datetime.now().isoformat()
        if row_count > 0:
            s3.upload_files(iter(self.filenames.values()), self.s3_client, self.config['s3_bucket'], self.config.get("compression"),
                            self.config.get('encryption_type'), self.config.get('encryption_key'), self.config.get('format'))
            emit_state(state)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    config_errors = utils.validate_config(config)
    if len(config_errors) > 0:
        LOGGER.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
        sys.exit(1)

    s3_client = s3.create_client(config)

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    target = TargetS3Parquet(config, s3_client)
    target.persist_messages(input_messages)

    LOGGER.debug("Exiting normally")


if __name__ == '__main__':
    main()
