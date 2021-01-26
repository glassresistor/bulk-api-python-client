from bulk_api_client.exceptions import InvalidQObject


class Q:
    """
    Q object that mimics the functionality of the Django Q object used to create
    a queryset. Operations happen automatically due to Python functionality. Use
    'output_filter' method to produce a dictionary containing the full chain.
    """

    AND = "and"
    OR = "or"
    NOT = "not"
    default = AND

    def __init__(self, _conn=None, _negated=False, **kwargs):
        self._conn = _conn or self.default
        self.negated = _negated
        self._children = list(kwargs.items())

    def _combine(self, object_on_right, conn):
        """
        Private combine method using self (Q) and Q object to the right,
        combining children if both contain the same operator or creating a new
        nest if the operators differ. Returns a new Q object that is the full
        chain.

        Args:
            self: Q object containing fields, filters on fields and operator
            object_on_right (Q): Q object to the right of self within the chain

        Returns:
            Q object representing full chain

        """
        if not isinstance(object_on_right, Q):
            raise InvalidQObject(
                "{} must be a Q object".format(object_on_right)
            )

        if not object_on_right._children:
            return self

        elif not self._children:
            return object_on_right

        q = type(self)()
        q._conn = conn
        q.add(self, conn)
        q.add(object_on_right, conn)
        return q

    def add(self, object_on_right, conn):
        """
        Given matching connections of binary operators on self and conn, combine
        or extend children of self or the object on the right. If the operators
        do not match, create a new Q object from the conn parameter whose
        children are self and object on the right.

        Args:
            self (Q obj)
            object_on_right (Q obj)
            conn: unary/binary operator

        Returns:
            Q obj

        """
        if self._conn == conn:
            # Add right object's children to self if they exist, if self has not
            # been negated, if conn param and right object's connector match,
            # and if right object has at least one child, else make right object
            # a child of self
            if (
                object_on_right._children
                and not self.negated
                and (
                    conn == object_on_right._conn
                    or len(object_on_right._children) == 1
                )
            ):
                self._children.extend(object_on_right._children)
                return self
            else:
                self._children.append(object_on_right)
                return object_on_right
        else:
            q = type(self)()
            q._children = self._children
            q._conn = self._conn
            q.negated = self.negated

            self._conn = conn
            self._children = [q, object_on_right]

            return object_on_right

    def __and__(self, object_on_right):
        return self._combine(object_on_right, self.AND)

    def __or__(self, object_on_right):
        return self._combine(object_on_right, self.OR)

    def __eq__(self, object_on_right):
        return (
            self.__class__ == object_on_right.__class__
            and self._conn == object_on_right._conn
            and self._children == object_on_right._children
        )

    def __invert__(self):
        """
        Creates a fresh Q object with connection "not" and sets its children as
        the self Q, whio is calling this method, using the add method. In dict
        form, this evaluates to:

        {"not": [{self_conn: [self_children]}]}

        Args:
            self (Q obj)

        Returns:
            Q obj

        """
        q = type(self)()
        q.add(self, self.NOT)
        q.remove_empty_child()
        q._negate()

        return q

    def _negate(self):
        """
        Flip the value of the bool property "negated," which aids the add method
        with unary operation
        """
        self.negated = not self.negated

    def remove_empty_child(self):
        """Remove empty child created on negation of Q object"""
        self._children = [self._children[-1]]

    def output_filter(self):
        """
        Creates a dictionary corresponding to the Q chain using the left-most Q
        object within the chain

        Args:
            self (Q obj)

        Returns:
            dict

        """
        return {
            self._conn: [
                c.output_filter() if isinstance(c, Q) else {c[0]: c[1]}
                for c in self._children
            ]
        }
