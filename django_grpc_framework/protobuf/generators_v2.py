import io
from collections import OrderedDict

from django.db import models
from django.template import loader
from rest_framework.utils import model_meta
from rest_framework.utils.field_mapping import ClassLookupDict


class ModelProtoGenerator:
    type_mapping = {
        # Numeric
        models.AutoField: 'int32',
        models.SmallIntegerField: 'int32',
        models.IntegerField: 'int32',
        models.BigIntegerField: 'int64',
        models.PositiveSmallIntegerField: 'int32',
        models.PositiveIntegerField: 'int32',
        models.FloatField: 'float',
        models.DecimalField: 'string',
        # Boolean
        models.BooleanField: 'bool',
        models.NullBooleanField: 'bool',
        # Date and time
        models.DateField: 'string',
        models.TimeField: 'string',
        models.DateTimeField: 'string',
        models.DurationField: 'string',
        # String
        models.CharField: 'string',
        models.TextField: 'string',
        models.EmailField: 'string',
        models.SlugField: 'string',
        models.URLField: 'string',
        models.UUIDField: 'string',
        models.GenericIPAddressField: 'string',
        models.FilePathField: 'string',
        # Default
        models.Field: 'string',
    }

    def __init__(self, model, field_names=None, package=None):
        self.model = model
        self.field_names = field_names
        if not package:
            package = f"{model.__name__.lower()}s"
        self.package = package
        self.type_mapping = ClassLookupDict(self.type_mapping)
        # Retrieve metadata about fields & relationships on the model class.
        self.field_info = model_meta.get_field_info(model)
        self._writer = _CodeWriter()

    def get_proto(self):
        self._writer.write_line('syntax = "proto3";')
        self._writer.write_line('')
        self._writer.write_line(f'package {self.package};')
        self._writer.write_line('')
        self._writer.write_line('import "google/api/annotations.proto";')
        self._writer.write_line('import "google/api/client.proto";')
        self._writer.write_line('import "google/api/field_behavior.proto";')
        self._writer.write_line('import "google/api/field_mask.proto";')
        self._writer.write_line('import "google/api/resource.proto";')
        self._writer.write_line('import "google/protobuf/empty.proto";')
        self._writer.write_line('')
        self._generate_service()
        self._writer.write_line('')
        self._generate_message()
        return self._writer.get_code()
        # return loader.render_to_string("model.proto", {}, request=None, using=None)

    def _generate_service(self):
        self._writer.write_line('service %sController {' % self.model.__name__)
        with self._writer.indent():
            self._writer.write_line(
                'rpc List(List%ssRequest) returns (List%ssResponse) {' %
                (self.model.__name__, self.model.__name__)
            )
            with self._writer.indent():
                self._writer.write_line('option (google.api.http) = {')
                with self._writer.indent():
                    self._writer.write_line(f'get: "/{self.model.__name__.lower()}s"')
                self._writer.write_line('};')
            self._writer.write_line('};')
            self._writer.write_line(
                'rpc Create(Create%sRequest) returns (%s) {' %
                (self.model.__name__, self.model.__name__)
            )
            with self._writer.indent():
                self._writer.write_line('option (google.api.http) = {')
                with self._writer.indent():
                    self._writer.write_line(f'post: "/{self.model.__name__.lower()}s"')
                    self._writer.write_line(f'body: "{self.model.__name__.lower()}"')
                self._writer.write_line('};')
                self._writer.write_line(f'option (google.api.method_signature) = "{self.model.__name__.lower()}";')
            self._writer.write_line('};')
            self._writer.write_line(
                'rpc Retrieve(Get%sRequest) returns (%s) {' %
                (self.model.__name__, self.model.__name__)
            )
            with self._writer.indent():
                self._writer.write_line('option (google.api.http) = {')
                with self._writer.indent():
                    self._writer.write_line(f'get: "/{self.model.__name__.lower()}s/{self.field_info.pk.name}"')
                self._writer.write_line('};')
                self._writer.write_line(f'option (google.api.method_signature) = "{self.field_info.pk.name}";')
            self._writer.write_line('};')
            self._writer.write_line(
                'rpc Update(Update%sRequest) returns (%s) {' %
                (self.model.__name__, self.model.__name__)
            )
            with self._writer.indent():
                self._writer.write_line('option (google.api.http) = {')
                with self._writer.indent():
                    self._writer.write_line(f'patch: "/{self.model.__name__.lower()}s/{self.field_info.pk.name}"')
                    self._writer.write_line(f'body: "{self.model.__name__.lower()}"')
                self._writer.write_line('};')
                self._writer.write_line(f'option (google.api.method_signature) = "{self.model.__name__.lower()},update_mask";')
            self._writer.write_line('};')
            self._writer.write_line(
                'rpc Destroy(Delete%sRequest) returns (google.protobuf.Empty) {' %
                self.model.__name__
            )
            with self._writer.indent():
                self._writer.write_line('option (google.api.http) = {')
                with self._writer.indent():
                    self._writer.write_line(f'delete: "/{self.model.__name__.lower()}s/{self.field_info.pk.name}"')
                self._writer.write_line('};')
                self._writer.write_line(f'option (google.api.method_signature) = "{self.field_info.pk.name}";')
            self._writer.write_line('};')
        self._writer.write_line('};')

    def _generate_message(self):
        self._writer.write_line('message %s {' % self.model.__name__)
        with self._writer.indent():
            for number, (field_name, proto_type) in enumerate(self.get_fields().items(), start=1):
                self._writer.write_line(f'{proto_type} {field_name} = {number};')
        self._writer.write_line('};')
        self._writer.write_line('')
        self._generated_list_response_message()
        self._writer.write_line('')
        self._generated_list_request_message()
        self._writer.write_line('')
        self._generated_create_request_message()
        self._writer.write_line('')
        self._generated_update_request_message()
        self._writer.write_line('')
        self._generated_delete_request_message()
        self._writer.write_line('')
        self._writer.write_line('message Get%sRequest {' % self.model.__name__)
        with self._writer.indent():
            pk_field_name = self.field_info.pk.name
            pk_proto_type = self.build_proto_type(
                pk_field_name, self.field_info, self.model
            )
            self._writer.write_line(f'{pk_proto_type} {pk_field_name} = 1;')
        self._writer.write_line('};')

    def _generated_list_response_message(self):
        self._writer.write_line('message List%ssResponse {' % self.model.__name__)
        with self._writer.indent():
            self._writer.write_line(f'// The {self.model.__name__.lower()}s.')
            self._writer.write_line(f'repeated {self.model.__name__} {self.model.__name__.lower()}s = 1')
            self._writer.write_line('')
            self._writer.write_line('// A token, which can be sent as `page_token` to retrieve the next page.')
            self._writer.write_line('// If this field is omitted, there are no subsequent pages.')
            self._writer.write_line('string next_page_token = 2;')
        self._writer.write_line('};')
        
    def _generated_list_request_message(self):
        self._writer.write_line('message List%ssRequest {' % self.model.__name__)
        with self._writer.indent():
            self._writer.write_line(f'// The maximum number of {self.model.__name__.lower()}s to return.')
            self._writer.write_line('// The service may return fewer than this value.')
            self._writer.write_line(f'// If unspecified, at most 50 {self.model.__name__.lower()}s will be returned.')
            self._writer.write_line('// The maximum value is 1000; values above 1000 will be coerced to 1000.')
            self._writer.write_line('int32 page_size = 1;')
            self._writer.write_line('')
            self._writer.write_line('// A page token, received from a previous `List` call.')
            self._writer.write_line('// Provide this to retrieve the subsequent page.')
            self._writer.write_line('// When paginating, all other parameters provided to `List` must match')
            self._writer.write_line('// The maximum value is 1000; values above 1000 will be coerced to 1000.')
            self._writer.write_line('string page_token = 2;')
            self._writer.write_line('')
            self._writer.write_line('int32 skip = 3;')
            self._writer.write_line('')
            self._writer.write_line('string order_by = 4;')
            self._writer.write_line('')
            self._writer.write_line('string filter = 5;')
            self._writer.write_line('')
            self._writer.write_line('bool show_deleted = 6;')
            
        self._writer.write_line('};')
        
    def _generated_create_request_message(self):
        self._writer.write_line('message Create%sRequest {' % self.model.__name__)
        with self._writer.indent():
            self._writer.write_line(f'// The {self.model.__name__.lower()} to create.')
            self._writer.write_line(f'{self.model.__name__} {self.model.__name__.lower()} = 1 [(google.api.field_behavior) = REQUIRED];')
        self._writer.write_line('};')
         
    def _generated_update_request_message(self):
        self._writer.write_line('message Update%sRequest {' % self.model.__name__)
        with self._writer.indent():
            self._writer.write_line(f'// The {self.model.__name__.lower()} to update.')
            self._writer.write_line(f'{self.model.__name__} {self.model.__name__.lower()} = 1 [(google.api.field_behavior) = REQUIRED];')
            self._writer.write_line('')
            self._writer.write_line('// The list of fields to update.')
            self._writer.write_line('google.protobuf.FieldMask update_mask = 2;')
        self._writer.write_line('};')

    def _generated_delete_request_message(self):
        self._writer.write_line('message Delete%sRequest {' % self.model.__name__)
        with self._writer.indent():
            self._writer.write_line(f'// The {self.model.__name__.lower()} to delete.')
            pk_field_name = self.field_info.pk.name
            pk_proto_type = self.build_proto_type(
                pk_field_name, self.field_info, self.model
            )
            self._writer.write_line(f'{pk_proto_type} {pk_field_name} = 1 [')
            with self._writer.indent():
                self._writer.write_line('(google.api.field_behavior) = REQUIRED,')
                self._writer.write_line('(google.api.resource_reference) = {')
                with self._writer.indent():
                    self._writer.write_line(f'type: "{self.model.__name__}"')
                self._writer.write_line('}];')
        self._writer.write_line('};')
       
    def get_fields(self):
        """
        Return the dict of field names -> proto types.
        """
        if model_meta.is_abstract_model(self.model):
            raise ValueError('Cannot generate proto for abstract model.')
        fields = OrderedDict()
        for field_name in self.get_field_names():
            if field_name in fields:
                continue
            fields[field_name] = self.build_proto_type(
                field_name, self.field_info, self.model
            )
        return fields

    def get_field_names(self):
        field_names = self.field_names or (
                        [self.field_info.pk.name]
                        + list(self.field_info.fields)
                        + list(self.field_info.forward_relations)
                    )
        return field_names

    def build_proto_type(self, field_name, field_info, model_class):
        if field_name in field_info.fields_and_pk:
            model_field = field_info.fields_and_pk[field_name]
            return self._build_standard_proto_type(model_field)
        elif field_name in field_info.relations:
            relation_info = field_info.relations[field_name]
            return self._build_relational_proto_type(relation_info)
        else:
            raise ValueError(
                f'Field name `{field_name}` is not valid for model `{model_class.__name__}`.'
            )

    def _build_standard_proto_type(self, model_field):
        if model_field.one_to_one and model_field.primary_key:
            info = model_meta.get_field_info(model_field.related_model)
            return self.build_proto_type(
                info.pk.name, info, model_field.related_model
            )
        else:
            return self.type_mapping[model_field]

    def _build_relational_proto_type(self, relation_info):
        info = model_meta.get_field_info(relation_info.related_model)
        to_field = info.pk.name
        if relation_info.to_field and not relation_info.reverse:
            to_field = relation_info.to_field
        proto_type = self.build_proto_type(
            to_field, info, relation_info.related_model
        )
        if relation_info.to_many:
            proto_type = f'repeated {proto_type}'
        return proto_type


class _CodeWriter:
    def __init__(self):
        self.buffer = io.StringIO()
        self._indent = 0

    def indent(self):
        return self

    def __enter__(self):
        self._indent += 1
        return self

    def __exit__(self, *args):
        self._indent -= 1

    def write_line(self, line):
        for _ in range(self._indent):
            self.buffer.write("    ")
        print(line, file=self.buffer)

    def get_code(self):
        return self.buffer.getvalue()