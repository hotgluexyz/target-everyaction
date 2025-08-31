"""EveryAction target sink class, which handles writing streams."""

from typing import Optional
from target_everyaction.client import EveryActionSink
import singer
from urllib.parse import urlencode

LOGGER = singer.get_logger()


class ContactsSink(EveryActionSink):
    """EveryAction target sink class."""

    name = "Contacts"
    endpoint = "people/findOrCreate"

    def map_fields(self, record: dict) -> dict:
        payload = {
            "firstName": record.get("first_name"),
            "lastName": record.get("last_name"),
            "middleName": record.get("middle_name"),
            "jobTitle": record.get("title"),
            "website": record.get("website"),
            "salutation": record.get("salutation"),
            "dateOfBirth": record.get("birthdate"),
            "occupation": record.get("occupation"),
            "vanId": record.get("id"),
        }

        if "email" in record:
            payload["emails"] = [
                {
                    "email": record["email"],
                }
            ]

        if "addresses" in record and isinstance(record["addresses"], list):
            payload["addresses"] = []
            for address in record["addresses"]:
                address_dict = {
                    "addressLine1": address.get("line1"),
                    "addressLine2": address.get("line2"),
                    "addressLine3": address.get("line3"),
                    "city": address.get("city"),
                    "stateOrProvince": address.get("state"),
                    "zipOrPostalCode": address.get("postal_code"),
                    "countryCode": address.get("country"),
                }
                payload["addresses"].append(address_dict)

        if "phone_numbers" in record and isinstance(record["phone_numbers"], list):
            payload["phones"] = []
            for phone in record["phone_numbers"]:
                phone_dict = {
                    "phoneNumber": phone.get("number"),
                    "phoneType": phone.get("type") if phone.get("type") in ["H", "W", "O"] else None,
                }
                payload["phones"].append(phone_dict)

        self.pending_codes = {}
        
        if record.get("lists"):
            self.pending_codes["activist"] = record["lists"]
            
        if record.get("lead_source"):
            self.pending_codes["source"] = record["lead_source"]
            
        if record.get("tags"):
            self.pending_codes["tags"] = record["tags"]

        return payload


    def find_by_van_id(self, van_id: str):
        """Find a person record by their VAN ID."""
        params = {
            "$expand": "phones,emails,addresses"
        }
        response = self.request_api(
            method="GET", 
            request_data=None,
            endpoint=f"people/{van_id}?{urlencode(params)}",
            validate_response=False
        )

        self.validate_response(response)

        payload = self._clean_contact_response(response.json())

        return payload


    def _clean_contact_response(self, payload):
        """
        There are certain fields returned by EveryAction in GET requests, that will fail POST requests if included
        """

        if payload.get("emails"):
            payload["emails"] = [
                {
                    "email": email.get("email"),
                    "type": email.get("type"),
                    "isPreferred": email.get("isPreferred")
                }
                for email in payload["emails"]
            ]
        return payload
        

    def find_by_email(self, email: str):
        """Find a person record by their email address."""
        response = self.request_api(
            method="POST", 
            request_data={"emails": [{"email": email}]}, 
            endpoint="people/find",
            validate_response=False
        )

        if response.status_code == 404 and 'Unmatched' in response.text:
            # Person not found, return None
            return None

        self.validate_response(response)

        # Find endpoint doesn't return all fields, so need to get record from vanID

        van_id = response.json().get("vanId")
        if not van_id:
            raise Exception(f"Unexpected response from find endpoint: {response.json()}")
        
        return self.find_by_van_id(van_id)


    def find_existing_contact(self, record: dict):
        """Find existing contact by VAN ID or email address."""
        van_id = record.get("vanId")
        email = record.get("emails", [])[0].get("email") if record.get("emails") else None
    
        existing_record = None
        
        if van_id:
            existing_record = self.find_by_van_id(van_id)
        elif email:
            existing_record = self.find_by_email(email)

        return existing_record
            



    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = self.map_fields(record)
        
        if self.config.get("only_upsert_empty_fields", False):
            existing_contact = self.find_existing_contact(record)
            if existing_contact:
                record["vanId"] = existing_contact["vanId"]

                for key, _ in record.items():
                    if existing_contact.get(key) not in [None, []]:
                        record[key] = existing_contact.get(key)
        
        return record
    
    
    def _get_or_create_code(self, code_payload: dict) -> Optional[str]:
        """Get existing code ID or create new code."""
        # Get existing codes
        response = self.request_api("GET", endpoint="codes")
        existing_codes = {}
        while True:
            if response.ok:
                data = response.json()
                existing_codes.update({
                    item["name"].lower(): item["codeId"] 
                    for item in data["items"]
                })
                if "nextPageLink" not in data:
                    break
                response = self.request_api("GET", 
                                          endpoint=f"codes?{data['nextPageLink'].split('?')[1]}")
            else:
                break

        # Check for existing code
        code_name = code_payload["name"].lower()
        if code_name in existing_codes:
            return existing_codes[code_name]

        # Create new code
        response = self.request_api("POST", 
                                  endpoint="codes",
                                  request_data=code_payload)
        return response.json() if response.ok else None

    def upsert_record(self, record: dict, context: dict):
        method = "POST"
        state_dict = {
            "is_updated": record.get("vanId") is not None
        }

        response = self.request_api(method, request_data=record, endpoint=self.endpoint)
        if response.status_code in [200, 201]:
            state_dict["success"] = True
            id = response.json().get("vanId")

            if hasattr(self, "pending_codes"):
                # Activist codes
                if self.pending_codes.get("activist"):
                    for code in self.pending_codes["activist"]:
                        payload = {
                            "responses": [{
                                "activistCodeId": code,
                                "action": "Apply",
                                "type": "ActivistCode"
                            }]
                        }
                        self.request_api("POST", endpoint=f"people/{id}/canvassResponses", 
                                       request_data=payload)

                # Handle both Source Codes and Tags
                for code_type, codes in self.pending_codes.items():
                    if code_type in ["source", "tags"]:
                        codes_list = [codes] if code_type == "source" else codes
                        for code in codes_list:
                            create_payload = {
                                "name": code,
                                "codeType": "SourceCode" if code_type == "source" else "Tag"
                            }
                            if code_type == "tags":
                                create_payload["supportedEntities"] = [{
                                    "name": "Contacts",
                                    "isSearchable": "true",
                                    "isApplicable": "true"
                                }]
                            
                            code_id = self._get_or_create_code(create_payload)
                            if code_id:
                                self.request_api("POST", endpoint=f"people/{id}/codes",
                                               request_data={"codeId": code_id})
        else:
            state_dict["success"] = False
            id = None

        LOGGER.info(f"Upsert result: id={id}, success={state_dict['success']}")
        return id, response.ok, state_dict
