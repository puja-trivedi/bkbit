from collections import defaultdict
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Type, Union, Tuple
import requests
from linkml_runtime.utils.schemaview import SchemaView as sv 
from bkbit.models import library_generation as lg

logging.basicConfig(
    filename="library_generation_translator_" + datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + ".log",
    format="%(levelname)s: %(message)s (%(asctime)s)",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

MODULE_DIR = Path(__file__).resolve().parent
LIBRARY_GENERATION_MODEL_PATH = MODULE_DIR / 'models' / 'library_generation.yaml'
BICAN_TO_NIMP_FILE_PATH = "../utils/bican_to_nimp_slots.csv"
API_URL_PREFIX = "https://brain-specimenportal.org/api/v1/nhash_ids/"
INFO_URL_SUFFIX = "info?id="
ANCESTORS_URL_SUFFIX = "ancestors?id="
DESCENDANTS_URL_SUFFIX = "descendants?id="
PARENTS_URL_SUFFIX = "parents?id="
CHILDREN_URL_SUFFIX = "children?id="
NHASH_ONLY_SUFFIX = "&nhash_only="
#! TODO: Update this mapping to nimp_category to class_name 
CATEGORY_TO_CLASS = {
    "librarypool": lg.LibraryPool,
    "libraryaliquot": lg.LibraryAliquot,
    "library": lg.Library,
    "amplifiedcdna": lg.AmplifiedCdna,
    "barcodedcellsample": lg.BarcodedCellSample,
    "enrichedcellsample": lg.EnrichedCellSample,
    "dissociatedcellsample": lg.DissociatedCellSample,
    "tissue": lg.TissueSample,
    "donor": lg.Donor,
    "specimendissectedroi": lg.DissectionRoiPolygon,
    "slab": lg.BrainSlab,
}
NIMP_NO_DATA_FOUND = {'error': 'No data found'}

# def parse_field_info(field_info) -> Tuple[bool, Union[str, None], Type, bool]:
#     required = field_info.is_required()
#     local_name_value = field_info.json_schema_extra.get('linkml_meta',{}).get('local_names',{}).get('NIMP', {}).get('local_name_value')

#     # Determine the type with priority to str and ignoring NoneType
#     types = [t for t in field_info.annotation.__args__ if t is not type(None)]
#     if str in types:
#         field_type = str
#     else:
#         field_type = types[0]


#     is_list = hasattr(field_type, '__origin__') and field_type.__origin__ is list

#     return required, local_name_value, field_type, is_list
def parse_field_info(field_info) -> Tuple[bool, Union[str, None], str, bool]:
    required = field_info.is_required()
    local_name_value = field_info.json_schema_extra.get('linkml_meta', {}).get('local_names', {}).get('NIMP', {}).get('local_name_value')

    # Determine the type with priority to str and ignoring NoneType
    types = [t for t in field_info.annotation.__args__ if t is not type(None)]
    
    # Default field_type to 'str' if it is in the types list, otherwise take the first type
    field_type = 'str' if str in types else types[0]

    is_list = False
    if hasattr(field_info.annotation, '__origin__') and field_info.annotation.__origin__ is list:
        is_list = True
        field_type = field_info.annotation.__args__[0].__name__

    return required, local_name_value, field_type, is_list


# class SpecimenPortal:
#     @staticmethod
#     def create_bican_to_nimp_mapping(csv_file):
#         """
#         Creates a mapping dictionary from a CSV file, where the keys are 'LinkML Slot or Attribute Name'
#         and the values are 'NIMP Variable Name'.

#         Parameters:
#             csv_file (str): The path to the CSV file.

#         Returns:
#             dict: A dictionary mapping 'LinkML Slot or Attribute Name' to 'NIMP Variable Name'.

#         """
#         bican_to_nimp_attribute_mapping = {}
#         nimp_to_bican_class_mapping = {}
#         bican_slots_per_class = defaultdict(set)

#         with open(csv_file, "r", encoding="utf-8") as file:
#             reader = csv.DictReader(file)
#             for row in reader:
#                 bican_name = (
#                     row["SubGroup/LinkML Class Name"].lower()
#                     + "_"
#                     + row["LinkML Slot or Attribute Name"].lower()
#                 )
#                 nimp_name = row["NIMP Variable Name"]
#                 bican_to_nimp_attribute_mapping[bican_name] = nimp_name
#                 nimp_to_bican_class_mapping[
#                     row["NIMP Category"].replace(" ", "").lower()
#                 ] = row["SubGroup/LinkML Class Name"].lower()
#                 bican_slots_per_class[row["SubGroup/LinkML Class Name"].lower()].add(
#                     row["LinkML Slot or Attribute Name"].lower()
#                 )
#         return (
#             bican_to_nimp_attribute_mapping,
#             nimp_to_bican_class_mapping,
#             bican_slots_per_class,
#         )

#     (
#         bican_to_nimp_attribute_mapping,
#         nimp_to_bican_class_mapping,
#         bican_slots_per_class,
#     ) = create_bican_to_nimp_mapping(BICAN_TO_NIMP_FILE_PATH)

#     def __init__(self, jwt_token):
#         self.logger = logger
#         self.jwt_token = jwt_token
#         self.generated_objects = {}

#     @staticmethod
#     def get_data(nhash_id, jwt_token):
#         """
#         Retrieve information of any record with a NHash ID in the system.

#         Parameters:
#             nhash_id (str): The NHash ID of the record to retrieve.
#             jwt_token (str): The JWT token for authentication.

#         Returns:
#             dict: The JSON response containing the information of the record.

#         Raises:
#             requests.exceptions.HTTPError: If there is an error retrieving the data.

#         """
#         headers = {"Authorization": f"Bearer {jwt_token}"}
#         response = requests.get(
#             f"{API_URL_PREFIX}{INFO_URL_SUFFIX}{nhash_id}",
#             headers=headers,
#             timeout=10,  # ? is this an appropriate timeout value?
#         )
#         if response.status_code == 200:
#             return response.json()

#         logger.critical("Error getting data for NHash ID = {nhash_id}. Status Code: {response.status_code}")
#         raise requests.exceptions.HTTPError(
#             f"Error getting data for NHash ID = {nhash_id}. Status Code: {response.status_code}"
#         )

#     @staticmethod
#     def get_ancestors(nhash_id, jwt_token, nhash_only=True, depth=1):
#         """
#         Retrieve information of all ancestors of a record with the given NHash ID.

#         Parameters:
#             nhash_id (str): The NHash ID of the record.
#             jwt_token (str): The JWT token for authentication.
#             nhash_only (bool): Flag indicating whether to retrieve only NHash IDs or complete record information. Default is True.
#             depth (int): The depth of ancestors to retrieve. Default is 1.

#         Returns:
#             dict: The JSON response containing information of all ancestors.

#         Raises:
#             requests.exceptions.HTTPError: If there is an error getting data for the NHash ID.

#         """
#         headers = {"Authorization": f"Bearer {jwt_token}"}

#         response = requests.get(
#             f"{API_URL_PREFIX}{ANCESTORS_URL_SUFFIX}{nhash_id}{NHASH_ONLY_SUFFIX}{nhash_only}",
#             headers=headers,
#             timeout=10,  # This is an appropriate timeout value.
#         )
#         if response.status_code == 200:
#             return response.json()
        
#         logger.critical("Error getting ancestors for NHash ID = {nhash_id}. Status Code: {response.status_code}")
#         raise requests.exceptions.HTTPError(
#             f"Error getting data for NHash ID = {nhash_id}. Status Code: {response.status_code}"
#         )

#     @staticmethod
#     def get_descendants(nhash_id, jwt_token, nhash_only=True, depth=1):
#         """
#         Retrieve information of all descendants of a record with the given NHash ID.

#         Parameters:
#             nhash_id (str): The NHash ID of the record.
#             jwt_token (str): The JWT token for authentication.
#             nhash_only (bool): Flag indicating whether to retrieve only NHash IDs or complete record information. Default is True.
#             depth (int): The depth of descendants to retrieve. Default is 1.

#         Returns:
#             dict: The JSON response containing information of all descendants.

#         Raises:
#             requests.exceptions.HTTPError: If there is an error getting data for the NHash ID.

#         """
#         headers = {"Authorization": f"Bearer {jwt_token}"}

#         response = requests.get(
#             f"{API_URL_PREFIX}{DESCENDANTS_URL_SUFFIX}{nhash_id}{NHASH_ONLY_SUFFIX}{nhash_only}",
#             headers=headers,
#             timeout=10,  # This is an appropriate timeout value.
#         )
#         if response.status_code == 200:
#             return response.json()
        
#         logger.critical("Error getting descendants for NHash ID = {nhash_id}. Status Code: {response.status_code}")
#         raise requests.exceptions.HTTPError(
#             f"Error getting data for NHash ID = {nhash_id}. Status Code: {response.status_code}"
#         )

#     @staticmethod
#     def get_children(nhash_id, jwt_token):
#         """
#         Retrieve information of all children of a record with the given NHash ID.

#         Parameters:
#             nhash_id (str): The NHash ID of the record.
#             jwt_token (str): The JWT token for authentication.

#         Returns:
#             dict: The JSON response containing information of all children.

#         Raises:
#             requests.exceptions.HTTPError: If there is an error getting data for the NHash ID.

#         """
#         headers = {"Authorization": f"Bearer {jwt_token}"}

#         response = requests.get(
#             f"{API_URL_PREFIX}{CHILDREN_URL_SUFFIX}{nhash_id}",
#             headers=headers,
#             timeout=10,  # This is an appropriate timeout value.
#         )
#         if response.status_code == 200:
#             return response.json()
        
#         logger.critical("Error getting children for NHash ID = {nhash_id}. Status Code: {response.status_code}")
#         raise requests.exceptions.HTTPError(
#             f"Error getting data for NHash ID = {nhash_id}. Status Code: {response.status_code}"
#         )
    
#     @staticmethod
#     def get_parents(nhash_id, jwt_token):
#         """
#         Retrieve information of all parents of a record with the given NHash ID.

#         Parameters:
#             nhash_id (str): The NHash ID of the record.
#             jwt_token (str): The JWT token for authentication.

#         Returns:
#             dict: The JSON response containing information of all parents.

#         Raises:
#             requests.exceptions.HTTPError: If there is an error getting data for the NHash ID.

#         """
#         headers = {"Authorization": f"Bearer {jwt_token}"}

#         response = requests.get(
#             f"{API_URL_PREFIX}{PARENTS_URL_SUFFIX}{nhash_id}",
#             headers=headers,
#             timeout=10,  # This is an appropriate timeout value.
#         )
#         if response.status_code == 200:
#             return response.json()
        
#         logger.critical("Error getting parents for NHash ID = {nhash_id}. Status Code: {response.status_code}")
#         raise requests.exceptions.HTTPError(
#             f"Error getting data for NHash ID = {nhash_id}. Status Code: {response.status_code}"
#         )

#     @classmethod
#     def generate_bican_object(cls, data, was_derived_from: list[str] = None):
#         """
#         Generate a Bican object based on the provided data.

#         Parameters:
#             data (dict): The data retrieved from the NIMP portal.
#             was_derived_from (list): A list of parent NHash IDs.

#         Returns:
#             The generated Bican object.

#         Raises:
#             None.

#         """
#         # TODO: check if NHash ID is not found
#         nhash_id = data.get("id")
#         category = data.get("category").replace(" ", "").lower()
#         bican_category = cls.nimp_to_bican_class_mapping.get(category)
#         if bican_category is None:
#             return None
#         bican_class = CATEGORY_TO_CLASS.get(category)
#         bican_object = bican_class(id="NIMP:" + nhash_id)
#         # handle was_derived_from attribute. type of this attribute can either be Optional[str] or Optional[List[str]]
#         if "List" in bican_class.__annotations__["was_derived_from"]:
#             bican_object.was_derived_from = [
#                 f"NIMP:{item}" for item in was_derived_from
#             ]
#         else:
#             bican_object.was_derived_from = (
#                 f"NIMP:{was_derived_from[0]}" if was_derived_from else None
#             )
#         class_attributes = SpecimenPortal.bican_slots_per_class.get(bican_category)
#         if class_attributes is not None:
#             for attribute in class_attributes:
#                 if (
#                     bican_category + "_" + attribute
#                 ) in SpecimenPortal.bican_to_nimp_attribute_mapping:
#                     bican_attribute_type = bican_class.__annotations__[attribute]
#                     value = data.get("record").get(
#                         SpecimenPortal.bican_to_nimp_attribute_mapping[
#                             bican_category + "_" + attribute
#                         ]
#                     )
#                     if value is not None and type(value) != bican_attribute_type:
#                         if (
#                             "AmplifiedCdnaRnaAmplificationPassFail"
#                             in bican_attribute_type
#                         ):
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "AmplifiedCdnaRnaAmplificationPassFail", value
#                             ) 
#                         elif "BarcodedCellSampleTechnique" in bican_attribute_type:
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "BarcodedCellSampleTechnique", value
#                             )
#                         elif (
#                             "DissociatedCellSampleCellPrepType" in bican_attribute_type
#                         ):
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "DissociatedCellSampleCellPrepType", value
#                             )
#                         elif (
#                             "DissociatedCellSampleCellLabelBarcode"
#                             in bican_attribute_type
#                         ):
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "DissociatedCellSampleCellLabelBarcode", value
#                             )
#                         elif "LibraryTechnique" in bican_attribute_type:
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "LibraryTechnique", value
#                             )
#                         elif "LibraryPrepPassFail" in bican_attribute_type:
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "LibraryPrepPassFail", value
#                             )
#                         elif "LibraryR1R2Index" in bican_attribute_type:
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "LibraryR1R2Index", value
#                             )
#                         elif "Sex" in bican_attribute_type:
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "Sex", value
#                             )
#                         elif "AgeAtDeathReferencePoint" in bican_attribute_type:
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "AgeAtDeathReferencePoint", value
#                             )
#                         elif "AgeAtDeathUnit" in bican_attribute_type:
#                             value = SpecimenPortal.__check_valueset_membership(
#                                 "AgeAtDeathUnit", value
#                             )
#                         elif "str" in bican_attribute_type:
#                             if "List" in bican_attribute_type:
#                                 pass
#                             else:
#                                 value = str(value)
#                         elif "int" in bican_attribute_type:
#                             value = int(float(value))
#                         elif "float" in bican_attribute_type:
#                             value = float(value)
#                         elif "bool" in bican_attribute_type:
#                             value = bool(value)
#                         else:
#                             value = None
#                     bican_object.__setattr__(attribute, value)
#         return bican_object

#     @classmethod
#     def generate_bican_object_new(cls, data, was_derived_from: list[str] = None):
#         """
#         Generate a Bican object based on the provided data.

#         Parameters:
#             data (dict): The data retrieved from the NIMP portal.
#             was_derived_from (list): A list of parent NHash IDs.

#         Returns:
#             The generated Bican object.

#         Raises:
#             None.

#         """
#         nhash_id = data.get("id")
#         category = data.get("category").replace(" ", "").lower()
#         bican_class = CATEGORY_TO_CLASS.get(category)
#         bican_object = bican_class(id="NIMP:" + nhash_id)
#         for attribute_name, attribute_metadata in bican_class.__fields__.items():
#             attribute_required = attribute_metadata.required
#             nimp_local_name = attribute_metadata.json_schema_extra.get('linkml_meta').get('local_names')
#             if nimp_local_name is not None:
#                 value = data.get("record").get(nimp_local_name)
#                 if value is not None:
#                     bican_object.__setattr__(attribute_name, value)
#     @staticmethod
#     def __check_valueset_membership(enum_name, nimp_value):
#         """
#         Check if the given value belongs to the specified enum.

#         Parameters:
#             enum_name (str): The name of the enum to check.
#             nimp_value: The value to check for membership in the enum.

#         Returns:
#             The enum member if the value belongs to the enum, None otherwise.
#         """
#         enum = lg.__dict__.get(enum_name)
#         if enum is not None:
#             valueset = {m.value: m for m in enum}
#             return valueset.get(nimp_value)
#         return None

#     def parse_nhash_id(self, nhash_id):
#         nimp_ancestor_pull = SpecimenPortal.get_ancestors(nhash_id, self.jwt_token)
#         #TODO: maybe in the future we can still add the object related to the nhash_id even if there is no ancestor data
#         if 'data' not in nimp_ancestor_pull:
#             logger.critical("No ancestor data found for NHash ID = %s", nhash_id)
#             return

#         # if nimp_ancestor_pull == NIMP_NO_DATA_FOUND:
#         #     logger.critical("No ancestor data found for NHash ID = %s", nhash_id)
#         #     ancestor_data = None
        
#         ancestor_data = nimp_ancestor_pull.get("data")
#         stack = [nhash_id]
#         while stack:
#             current_nhash_id = stack.pop()
#             if current_nhash_id not in self.generated_objects:
#                 nimp_data_pull = SpecimenPortal.get_data(current_nhash_id, self.jwt_token)
#                 if 'data' not in nimp_data_pull:
#                     logger.critical("No data found for NHash ID = %s", current_nhash_id)
#                     return
#                 data = nimp_data_pull.get("data")
#                 parents = (
#                     ancestor_data.get(current_nhash_id).get("edges").get("has_parent")
#                 )
#                 bican_object = self.generate_bican_object(data, parents)
#                 if bican_object is not None:
#                     self.generated_objects[current_nhash_id] = bican_object
#                 if parents is not None:
#                     stack.extend(parents)

#     def parse_nhash_id_descendants(self, nhash_id):
#         nimp_descendant_fetch = SpecimenPortal.get_descendants(nhash_id, self.jwt_token)
#         if 'data' not in nimp_descendant_fetch:
#             logger.critical("No descendant data found for NHash ID = %s", nhash_id)
#             return
#         descendant_data = nimp_descendant_fetch.get("data")
#         for nhash_id_key, value in descendant_data.items():
#             if nhash_id_key not in self.generated_objects:
#                 nimp_data_fetch = SpecimenPortal.get_data(nhash_id_key, self.jwt_token)
#                 if 'data' not in nimp_data_fetch:
#                     logger.critical("No data found for NHash ID = %s", nhash_id_key)
#                     return
#                 data = nimp_data_fetch.get("data")
#                 parents = value.get("edges").get("has_children")
#                 bican_object = self.generate_bican_object(data, parents)
#                 if bican_object is not None:
#                     self.generated_objects[nhash_id_key] = bican_object



#     def serialize_to_jsonld(
#         self, output_file: str, exclude_none: bool = True, exclude_unset: bool = False
#     ):
#         """
#         Serialize the object and write it to the specified output file.

#         Parameters:
#             output_file (str): The path of the output file.

#         Returns:
#             None
#         """
#         with open(output_file, "w", encoding="utf-8") as f:
#             data = []
#             for obj in self.generated_objects.values():
#                 # data.append(obj.to_dict(exclude_none=exclude_none, exclude_unset=exclude_unset))
#                 data.append(obj.__dict__)
#             output_data = {
#                 "@context": "https://raw.githubusercontent.com/brain-bican/models/main/jsonld-context-autogen/library_generation.context.jsonld",
#                 "@graph": data,
#             }
#             f.write(json.dumps(output_data, indent=2))


# if __name__ == "__main__":
#     pass
#     ## EXAMPLE #1 ##
#     # sp = SpecimenPortal(
#     #     "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxMTAsImV4cCI6MTcxNTc1NzUzN30.2CsWyCHwtOAd4NnOUMinhgtTk86z0ydh0T5__rfh824"
#     # )
#     # sp.parse_nhash_id('AC-ATDJAH472237')

#     ## EXAMPLE #2 ##
#     # token = "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxMTAsImV4cCI6MTcxNTc1NzUzN30.2CsWyCHwtOAd4NnOUMinhgtTk86z0ydh0T5__rfh824"
#     # sp = SpecimenPortal(
#     #     token
#     # )
#     # LIMIT = 10
#     # with open("example_library_pool_data.csv", "r", encoding="utf-8") as file:
#     #     reader = csv.DictReader(file)
#     #     row_number = 1
#     #     for row in reader:
#     #         print(f'Processing LP: {row["NHash ID"]}')
#     #         sp.parse_nhash_id(row["NHash ID"])
#     #         sp.serialize_to_jsonld("output_" + row["NHash ID"] + ".jsonld")
#     #         if row_number == LIMIT:
#     #             break
#     #         row_number += 1