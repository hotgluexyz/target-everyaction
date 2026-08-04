"""EveryAction target sink class, which handles writing streams."""

from typing import Any, Dict, Optional

import singer
from hotglue_etl_exceptions import InvalidPayloadError

from target_everyaction.client import EveryActionSink

LOGGER = singer.get_logger()

# Writable Person fields for POST /people/findOrCreate.
# Mirrors tap people/contacts schemas; omits read-only summary expansions.
_PERSON_WRITE_FIELDS = (
    "vanId",
    "firstName",
    "middleName",
    "lastName",
    "suffix",
    "title",
    "contactMode",
    "organizationContactCommonName",
    "organizationContactOfficialName",
    "salutation",
    "formalSalutation",
    "additionalSalutation",
    "envelopeName",
    "formalEnvelopeName",
    "additionalEnvelopeName",
    "contactMethodPreferenceCode",
    "nickname",
    "website",
    "professionalSuffix",
    "party",
    "employer",
    "occupation",
    "jobTitle",
    "sex",
    "dateOfBirth",
    "emails",
    "phones",
    "addresses",
    "recordedAddresses",
    "identifiers",
    "codes",
    "customFields",
    "notes",
    "customProperties",
)


def _drop_empty(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys whose values are None or empty containers."""
    cleaned = {}
    for key, value in payload.items():
        if value is None:
            continue
        if value == "" or value == [] or value == {}:
            continue
        cleaned[key] = value
    return cleaned


def _nested_id(value: Any, *keys: str) -> Optional[Any]:
    """Return the first present key from a nested object, or the scalar itself."""
    if isinstance(value, dict):
        for key in keys:
            if value.get(key) is not None:
                return value[key]
        return None
    return value


class ContactsSink(EveryActionSink):
    """Unified Contacts sink (NewMode CRM mapping → findOrCreate)."""

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
                if not data.get("nextPageLink"):
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
        state_dict = dict()

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


class PeopleSink(EveryActionSink):
    """Payees / vendors (native EveryAction Person → POST /people/findOrCreate).

    Accepts the same native field names as the tap ``people`` / ``contacts``
    streams (firstName, emails, phones, addresses, employer, …).
    """

    name = "People"
    endpoint = "people/findOrCreate"

    def map_fields(self, record: dict) -> dict:
        payload = {
            field: record[field]
            for field in _PERSON_WRITE_FIELDS
            if field in record and record[field] is not None
        }
        return _drop_empty(payload)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return self.map_fields(record)

    def upsert_record(self, record: dict, context: dict):
        state_dict: Dict[str, Any] = {}
        response = self.request_api("POST", request_data=record, endpoint=self.endpoint)
        if response.status_code in [200, 201]:
            state_dict["success"] = True
            record_id = response.json().get("vanId")
        else:
            state_dict["success"] = False
            record_id = None

        LOGGER.info(f"People upsert result: id={record_id}, success={state_dict['success']}")
        return record_id, response.ok, state_dict


class DisbursementsSink(EveryActionSink):
    """AP disbursements (POST /disbursements create-or-update).

    Required: contact.vanId, designation.designationId, dateIssued, amount.
    Also accepts flat ``vanId`` / ``designationId`` / ``date`` from the
    ``recent_disbursements`` list shape.
    """

    name = "Disbursements"
    endpoint = "disbursements"

    def map_fields(self, record: dict) -> dict:
        van_id = _nested_id(record.get("contact"), "vanId") or record.get("vanId")
        designation_id = (
            _nested_id(record.get("designation"), "designationId")
            or record.get("designationId")
        )
        date_issued = record.get("dateIssued") or record.get("date")

        if van_id is None:
            raise InvalidPayloadError("Disbursement requires contact.vanId (or vanId)")
        if designation_id is None:
            raise InvalidPayloadError(
                "Disbursement requires designation.designationId (or designationId)"
            )
        if not date_issued:
            raise InvalidPayloadError("Disbursement requires dateIssued (or date)")
        if record.get("amount") is None:
            raise InvalidPayloadError("Disbursement requires amount")

        payload: Dict[str, Any] = {
            "contact": {"vanId": int(van_id)},
            "designation": {"designationId": int(designation_id)},
            "dateIssued": date_issued,
            "amount": record.get("amount"),
            "batchCode": record.get("batchCode"),
            "checkDate": record.get("checkDate"),
            "checkNumber": record.get("checkNumber"),
            "note": record.get("note") or record.get("purpose"),
            "codes": record.get("codes"),
            "disclosureFieldValues": record.get("disclosureFieldValues"),
            "linkedCreditCardPaymentDisbursementId": record.get(
                "linkedCreditCardPaymentDisbursementId"
            ),
            "linkedReimbursementDisbursementId": record.get(
                "linkedReimbursementDisbursementId"
            ),
        }

        # Include id when updating an existing disbursement.
        disbursement_id = record.get("disbursementId") or record.get(
            "contactsDisbursementId"
        )
        if disbursement_id is not None:
            payload["disbursementId"] = int(disbursement_id)

        return _drop_empty(payload)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return self.map_fields(record)

    def upsert_record(self, record: dict, context: dict):
        state_dict: Dict[str, Any] = {}
        response = self.request_api("POST", request_data=record, endpoint=self.endpoint)
        if response.status_code in [200, 201]:
            state_dict["success"] = True
            body = response.json() if response.text else {}
            record_id = body.get("disbursementId") if isinstance(body, dict) else body
        else:
            state_dict["success"] = False
            record_id = None

        LOGGER.info(
            f"Disbursement upsert result: id={record_id}, success={state_dict['success']}"
        )
        return record_id, response.ok, state_dict
