import os
from django.core.management.base import BaseCommand, CommandError
from django.utils.module_loading import import_string

from django_grpc_framework.protobuf.generators_v3 import ModelProtoGenerator


class Command(BaseCommand):
    help = "Generates proto."
    operations = ["", "list", "create", "retrieve", "update", "delete"]

    def add_arguments(self, parser):
        parser.add_argument(
            'filepath', nargs="?",
            help='the generated proto file path'
        )
        parser.add_argument(
            '--model', dest='model', type=str, required=True,
            help='dotted path to a model class',
        )
        parser.add_argument(
            '--fields', dest='fields', default=None, type=str,
            help='specify which fields to include, comma-seperated'
        )
        parser.add_argument(
            '--filename', dest='filename', default=None, type=str,
            help='the generated proto file name'
        )

    def handle(self, *args, **options):
        model = import_string(options['model'])
        fields = options['fields'].split(',') if options['fields'] else None
        filepath = f"{(options['filepath'] or ".").rstrip("/")}/protos"
        generatedpath = f"{(options['filepath'] or ".").rstrip("/")}/generated"
        filename= options['filename'] or model.__name__.lower()
        try:
            os.mkdir(filepath)
            print(f"Directory '{filepath}' created successfully.")
        except FileExistsError:
            print(f"Directory '{filepath}' already exists.")
        except PermissionError:
            raise CommandError(f"Permission denied: Unable to create '{filepath}'.")
        except Exception as e:
            raise CommandError(f"An error occurred: {e}")
        try:
            os.mkdir(generatedpath)
            print(f"Directory '{generatedpath}' created successfully.")
        except FileExistsError:
            print(f"Directory '{generatedpath}' already exists.")
        except PermissionError:
            raise CommandError(f"Permission denied: Unable to create '{generatedpath}'.")
        except Exception as e:
            raise CommandError(f"An error occurred: {e}")
        for operation in self.operations:
            _filename= f"{operation}_{filename}".strip("_")
            generator = ModelProtoGenerator(
                model=model,
                field_names=fields,
                package=f"{operation}_{filename}s".strip("_"), 
                packagebase=f"{filename}s",
                operation=operation
            )
            proto = generator.get_proto()
            with open(f"{filepath}/{_filename}.proto", 'w') as f:
                f.write(proto)
                # print(f"Generated {_filename}.proto from model {model.__name__} into {filepath}")
                print(f"python -m grpc_tools.protoc --proto_path={filepath} --python_out={generatedpath} --grpc_python_out={generatedpath} {_filename}.proto")
            