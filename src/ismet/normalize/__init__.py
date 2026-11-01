"""Vendor payload to model mapping, one module per vendor.

Adapters shape requests and call in here. These functions do no I/O and take
``received`` explicitly, so each field mapping can be tested against a recorded
payload. A payload that does not have the documented shape raises a
non-retryable :class:`~ismet.errors.TransportError`.
"""
