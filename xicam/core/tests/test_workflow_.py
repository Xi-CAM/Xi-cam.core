import pytest

from xicam.core.execution.workflow import Graph
from xicam.plugins import OperationPlugin


@pytest.fixture
def graph():
    return Graph()


@pytest.fixture
def operation():
    def test_func(in1, in2):
        return in1 + in2
    return OperationPlugin(test_func, output_names=("out1", "out2"))


class TestGraph:

    def test_add_operation(self, graph, operation):
        graph.add_operation(operation)
        assert graph.operations == [operation]

    def test_add_operations(self, graph, operation):
        operations = [operation, operation]
        graph.add_operations(operations)
        assert graph.operations == operations

    def test_insert_operation(self, graph, operation):
        graph.insert_operation(0, operation)
        op2 = operation
        graph.insert_operation(0, op2)
        assert graph.operations == [op2, operation]

    def test_insert_operation(self, grpah, opera):

        # graph.insert_operation(-1, operation)
        # graph.insert_operation(100, operation)
        # graph.insert(0, 3)

    def test_add_link(self):
        assert False

    def test_add_link_missing_output(self, graph, operation):
        with pytest.raises(NameError):
            op2 = operation
            graph.add_link(source=operation, dest=op2, source_param="in1", dest_param="dne")

    def test_add_link_missing_input(self, graph, operation):
        with pytest.raises(NameError):
            op2 = operation
            graph.add_linke(source=operation, dest=op2, source_param="dne", dest_param="out1")

    def test_remove_link(self):
        assert False

    def test_get_inbound_links(self):
        assert False

    def test_get_outbound_links(self):
        assert False

    def test_clear_operation_links(self):
        assert False

    def test_clear_links(self):
        assert False

    def test_clear_operations(self):
        assert False

    def test_remove_operation(self):
        assert False

    def test_as_dask_graph(self):
        assert False

    def test_operations(self):
        assert False

    def test_links(self):
        assert False

    def test_operation_links(self):
        assert False

    def test_set_disabled(self):
        assert False

    def test_toggle_disabled(self):
        assert False

    def test_auto_connect_all(self):
        assert False

    def test_notify(self):
        graph = Graph()
        check.is_none(graph.notify())
