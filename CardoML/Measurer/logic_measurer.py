from typing import List

from CardoExecutor.Contract.IWorkflowFactory import IWorkflowFactory
from CardoExecutor.Workflows.DagSubWorkflow import DagSubWorkflow
from CardoExecutor.Contract.IStep import IStep

from CardoML.Common.Steps import IdCounts, Intersection, CalculatePrecision


class LogicMeasurer(IWorkflowFactory):
	"""
	Parameters
	----------
	ids : List
		list of ids to do the counts and the precision on

	intersection_dataset : List[IStep] (List[Reader]), default None
		the dataframe (aka GT) that used for the precision and intersection count.

	calc_precision : bool, default: True
		whether to calc precision.

	precision_column : str, default None
		if given keep the precision calculated as a column under the name given.

	Example
	-------
	gt = OracleReader(gt_query,conn)
	measure_workflow = LogicMeasurer(ids = ['id1','id2'],intersection_datasets = [gt]).create_workflow()
	logics = LogicWorkflowsFactory('my_logics', should_write = False, sub_workflow = measure_workflow)

	"""
	def __init__(self, ids: List, intersection_datasets: List[IStep] = None, calc_precision: bool = True,
				 precision_column: str = None) -> None:
		self.ids = ids
		self.intersection_datasets = intersection_datasets
		self.calc_precision = calc_precision
		self.precision_column = precision_column

	def create_workflow(self) -> DagSubWorkflow:
		workflow = DagSubWorkflow(name='LogicMeasurer')
		for id in self.ids:
			counts = IdCounts(id)
			workflow.add_last(counts)
		for intersection_dataset in self.intersection_datasets:
			intersection = Intersection(self.ids, 'gt_intersections')
			intersection_dataset = intersection_dataset()
			workflow.add_after([intersection], [counts, intersection_dataset])
			if self.calc_precision:
				workflow.add_after([CalculatePrecision(self.ids[0], self.ids[1], precision_column=self.precision_column,
										log_type='logics_precision')],[intersection, intersection_dataset])
				workflow.add_after([CalculatePrecision(self.ids[0], self.ids[1], precision_column=self.precision_column,
									   friendly_precision=True, log_type='logics_friendly_precision')],[intersection, intersection_dataset])
				workflow.add_after([CalculatePrecision(self.ids[1], self.ids[0], precision_column=self.precision_column,
									   log_type='logics_precision')],[intersection, intersection_dataset])
				workflow.add_after([CalculatePrecision(self.ids[1], self.ids[0], precision_column=self.precision_column,
									   friendly_precision=True, log_type='logics_friendly_precision')],[intersection, intersection_dataset])

		return workflow
