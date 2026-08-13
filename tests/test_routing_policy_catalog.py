import unittest

from smart_routing.routing_policy_catalog import (
    ROUTING_POLICY_VALUES,
    routing_policy_description,
    routing_policy_label,
)


class RoutingPolicyCatalogTests(unittest.TestCase):
    def test_six_supported_policies_have_human_readable_ui_text(self) -> None:
        self.assertEqual(6, len(ROUTING_POLICY_VALUES))
        for value in ROUTING_POLICY_VALUES:
            self.assertNotEqual(value, routing_policy_label(value))
            self.assertTrue(routing_policy_description(value))

    def test_unknown_policy_is_explicitly_marked(self) -> None:
        self.assertEqual("기존/미등록 정책", routing_policy_label("unknown/v1"))
        self.assertEqual("등록되지 않은 정책입니다. 정책 목록에서 다시 선택해 주세요.", routing_policy_description("unknown/v1"))


if __name__ == "__main__":
    unittest.main()
