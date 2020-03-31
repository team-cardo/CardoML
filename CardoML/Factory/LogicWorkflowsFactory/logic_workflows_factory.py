from abc import ABCMeta, abstractmethod
from typing import Optional, List, Iterator

from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Workflows.DagSubWorkflow import DagSubWorkflow
from CardoExecutor.Workflows.DagWorkflow import DagWorkflow
from CardoLibs.IO import HiveReader, HiveWriter

from CardoML.Common.Steps.PersistStep.persist_step import PersistStep
from CardoML.Factory.Workflows.WorkflowsFactory import WorkflowsFactory


class LogicWorkflowsFactory(WorkflowsFactory, metaclass=ABCMeta):
    def __init__(self,
                 workflows_name: str,
                 *extra_steps: IStep,
                 should_write: bool = False,
                 should_persist: bool = False,
                 connection_step: IStep = None,
                 sub_workflow: DagSubWorkflow = None,
                 wanted_logics: Optional[List[str]] = None,
                 unwanted_logics: Optional[List[str]] = None):
        """
        :param workflows_name: name of the workflow
        :param extra_steps: apply steps to the end every workflow
        :param should_write: write the final result to hive (tables name are the workflow name)
        :param should_persist: persist instead of writing and reading
        :param connection_step: optional connection step between the workflow and the sub_workflow
        :param sub_workflow: concat steps to the end of each workflow (ex: step that measure stuff)
        :param wanted_logics: run specific workflows
        :param unwanted_logics: don't run specific workflows

        Example Usage:
            class ExampleWorkflowsFactory(LogicWorkflowsFactory):
                def __init__(self):
                    super().__init__(WORKFLOW_NAME,
                                     FirstExtraStep(), SecondExtraStep(),
                                     should_write = False,
                                     wanted_logics = ['LogicA', ...],
                                     unwanted_logics = ['LogicB', ...])
        OR

        class ExampleWorkflowsFactory(LogicWorkflowsFactory) :
            def create_workflows(self)->Iterator[DagWorkflow]:
                return [Workflow1(),
                        Workflow2(),
                        Workflow3(),
                        self.create_workflow(SimpleStep())]
        Main
        ----
        measure_workflow = LogicMeasurer(ids = ['id1','id2'],intersection_datasets = gt).create_workflow()
        logics = ExampleWorkflowsFactory('my_logics', should_write = False,should_persist = True, sub_workflow = measure_workflow)
        """
        super().__init__(wanted_logics, unwanted_logics, should_write)
        self.workflows_name = workflows_name
        self.should_persist = should_persist
        self.connection_step = connection_step
        self.sub_workflow = sub_workflow
        self.extra_steps = extra_steps

    def get_all_workflows(self) -> Iterator[DagWorkflow]:
        return map(self.append_sub_workflow,
                   self.create_workflows())

    @abstractmethod
    def create_workflows(self) -> Iterator[DagWorkflow]:
        """
        List of workflows to execute,
        Example:

            def create_workflows(self):
                return [Workflow1().create_workflow(),
                        Workflow2().create_workflow(),
                        self.create_workflow(Step1(), ...)]

        :return:
        """
        raise NotImplementedError

    @staticmethod
    def create_workflow(logic: IStep, *extension_steps: IStep) -> DagWorkflow:
        wf = DagWorkflow(logic.__class__.__name__)
        wf.add_last(logic)
        for extension_step in extension_steps:
            wf.add_last(extension_step)
        return wf

    def append_sub_workflow(self, wf: DagWorkflow) -> DagWorkflow:
        for extra_step in self.extra_steps:
            wf.add_last(extra_step)
        if self.should_write:
            self.__add_writing(wf)
        elif self.should_persist:
            wf.add_last(PersistStep())
        if self.sub_workflow:
            if not self.connection_step:
                wf.append_workflow(self.sub_workflow, wf.get_before())
            else:
                wf.add_last(self.connection_step)
                wf.append_workflow(self.sub_workflow)
        return wf

    def __add_writing(self, wf: DagWorkflow):
        wf.add_last(HiveWriter(f"{self.workflows_name}.{wf.name}"))
        wf.add_last(HiveReader(f"{self.workflows_name}.{wf.name}"))
