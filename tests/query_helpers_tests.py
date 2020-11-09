import pytest

from bulk_api_client.query_helpers import Q
from bulk_api_client.exceptions import InvalidQObject


def test_init_q_obj():
    q_obj = Q(test_field="test")
    assert q_obj.output_filter() == {"AND": [{"test_field": "test"}]}


def test_q_obj_chain():
    # basic q chain AND
    q1 = Q(field1=1) & Q(field2=2)
    q1_or = Q(field1=1) | Q(field2=2)

    # chain var AND Q obj
    foo = Q(field1=1) & Q(field2=2)
    q2 = foo & Q(field3=3)

    # chain AND chain
    bar = Q(field4=4) & Q(field5=5)
    q3 = foo & bar

    # chain AND empty Q
    q4 = q1 & Q()

    # multiple params
    q5 = Q(field6=6, field7=7)

    # chain AND multi, show that multiple converts into individual params
    q6 = q3 & q5

    # chain OR multi
    q7 = q3 | q5

    assert q1.output_filter() == {"AND": [{"field1": 1}, {"field2": 2}]}
    assert q1_or.output_filter() == {"OR": [{"field1": 1}, {"field2": 2}]}
    assert q2.output_filter() == {
        "AND": [{"field1": 1}, {"field2": 2}, {"field3": 3}]
    }
    assert q3.output_filter() == {
        "AND": [{"field1": 1}, {"field2": 2}, {"field4": 4}, {"field5": 5}]
    }
    assert q4.output_filter() == {"AND": [{"field1": 1}, {"field2": 2}]}
    assert q5.output_filter() == {"AND": [{"field6": 6}, {"field7": 7}]}
    assert q6.output_filter() == {
        "AND": [
            {"field1": 1},
            {"field2": 2},
            {"field4": 4},
            {"field5": 5},
            {"field6": 6},
            {"field7": 7},
        ]
    }
    assert q7.output_filter() == {
        "OR": [
            {
                "AND": [
                    {"field1": 1},
                    {"field2": 2},
                    {"field4": 4},
                    {"field5": 5},
                ]
            },
            {"AND": [{"field6": 6}, {"field7": 7}]},
        ]
    }


@pytest.mark.parametrize(
    "obj,error",
    [
        (1, InvalidQObject),
        (1.1, InvalidQObject),
        (None, InvalidQObject),
        ({"filter": "some_filter"}, InvalidQObject),
        ([1, 2, 3], InvalidQObject),
    ],
)
def test_init_q_obj_failure(obj, error):
    with pytest.raises(error):
        Q(test1=1) & obj
    with pytest.raises(error):
        Q(test1=1) | obj
