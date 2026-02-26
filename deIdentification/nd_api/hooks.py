from worker.models import Task, Chain
from nd_api.models.table_details import TableDetailsModel


def de_identification_failure_hook_for_table(chain_obj: Chain):
    table_id = TableDetailsModel.get_table_id_from_chain_reference_uuid(
        chain_obj.reference_uuid
    )
    table_details_obj = TableDetailsModel.objects.get(id=table_id)
    table_details_obj.marked_as_failed()
    table_details_obj.save()

def qc_failure_hook_for_table(chain_obj: Chain):
    table_id = TableDetailsModel.get_table_id_from_chain_reference_uuid(
        chain_obj.reference_uuid
    )
    table_details_obj = TableDetailsModel.objects.get(id=table_id)
    table_details_obj.update_qc_status('failed')
    table_details_obj.save()