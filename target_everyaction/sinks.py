"""EveryAction target sink class, which handles writing streams."""

from typing import Optional
from target_everyaction.client import EveryActionSink
import singer

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
                    "phoneType": phone.get("type"),
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

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return self.map_fields(record)

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

    def _get_all_activist_codes(self) -> dict:
        """Get all activist codes and cache them in memory."""
        all_codes = {}
        params = {
            "statuses": "Active,Archived",
            "$top": 200
        }
        
        response = self.request_api("GET", endpoint="activistCodes", params=params)
        while True:
            if response.ok:
                data = response.json()
                # Store codes with lowercase names for case-insensitive lookup
                for item in data["items"]:
                    all_codes[item["name"].lower()] = item["activistCodeId"]
                    
                if "nextPageLink" not in data:
                    break
                    
                next_page = data["nextPageLink"].split("?")[1]
                response = self.request_api("GET", endpoint=f"activistCodes?{next_page}")
            else:
                break
        
        return all_codes

    def _process_activist_codes(self, person_id: int, code_names: list, cached_codes: dict):
        """Process activist codes for a person."""
        missing_codes = []
        
        # Build payload for existing codes
        responses = []
        for code_name in code_names:
            code_id = cached_codes.get(code_name.lower())
            if code_id:
                responses.append({
                    "activistCodeId": code_id,
                    "action": "Apply",
                    "type": "ActivistCode"
                })
            else:
                missing_codes.append(code_name)
                LOGGER.warning(f"Activist code '{code_name}' not found")
        
        # Apply existing codes in a single request if any exist
        if responses:
            payload = {"responses": responses}
            self.request_api(
                "POST",
                endpoint=f"people/{person_id}/canvassResponses",
                request_data=payload
            )
        
        return missing_codes

    def upsert_record(self, record: dict, context: dict):
        method = "POST"
        state_dict = dict()

        response = self.request_api(method, request_data=record, endpoint=self.endpoint)
        if response.status_code in [200, 201]:
            state_dict["success"] = True
            id = response.json().get("vanId")

            if hasattr(self, "pending_codes"):
                # Activist codes
                if self.pending_codes.get("activist"):
                    # Get all activist codes once
                    cached_codes = self._get_all_activist_codes()
                    # Process all codes for this person
                    missing_codes = self._process_activist_codes(
                        id, 
                        self.pending_codes["activist"],
                        cached_codes
                    )

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
