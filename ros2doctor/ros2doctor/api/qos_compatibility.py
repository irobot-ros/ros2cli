# Copyright 2021 Open Source Robotics Foundation, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum
import re

from rclpy.duration import Duration
from rclpy.qos import QoSDurabilityPolicy
from rclpy.qos import QoSLivelinessPolicy
from rclpy.qos import QoSReliabilityPolicy
from ros2cli.node.strategy import NodeStrategy
from ros2doctor.api import DoctorCheck
from ros2doctor.api import DoctorReport
from ros2doctor.api import get_topic_names
from ros2doctor.api import Report
from ros2doctor.api import Result
from ros2doctor.api.format import doctor_error
from ros2doctor.api.format import doctor_warn

class QoSCompatibility(Enum):
    OK = 0
    WARNING = 1
    ERROR = 2

def qos_check_compatible(pub, sub):
    # This method is adapted from https://github.com/ros2/rmw_dds_common/blob/d0b29f1e180ab08fdbd9870e29c9382f3aaff130/rmw_dds_common/src/qos.cpp#L74
    compatibility = QoSCompatibility.OK

    # Best effort publisher and reliable subscription
    if pub.reliability == QoSReliabilityPolicy.BEST_EFFORT and sub.reliability == QoSReliabilityPolicy.RELIABLE:
        compatibility = QoSCompatibility.ERROR
        return (compatibility, "ERROR: Best effort publisher and reliable subscription;")

    # Volatile publisher and transient local subscription
    if pub.durability == QoSDurabilityPolicy.VOLATILE and sub.durability == QoSDurabilityPolicy.TRANSIENT_LOCAL:
        compatibility = QoSCompatibility.ERROR
        return (compatibility, "ERROR: Volatile publisher and transient local subscription;")

    deadline_default = Duration()

    # No deadline for publisher and deadline for subscription
    if pub.deadline == deadline_default and sub.deadline != deadline_default:
        compatibility = QoSCompatibility.ERROR
        return (compatibility, "ERROR: Subscription has a deadline, but publisher does not;")

    # Subscription deadline is less than publisher deadline
    if pub.deadline != deadline_default and sub.deadline != deadline_default and sub.deadline < pub.deadline:
        compatibility = QoSCompatibility.ERROR
        return (compatibility, "ERROR: Subscription deadline is less than publisher deadline;")

    # Automatic liveliness for publisher and manual by topic for subscription
    if pub.liveliness == QoSLivelinessPolicy.AUTOMATIC and sub.liveliness == QoSLivelinessPolicy.MANUAL_BY_TOPIC:
        compatibility = QoSCompatibility.ERROR
        return (compatibility, "ERROR: Publisher's liveliness is automatic and subscription's is manual by topic;")    

    lease_default = Duration()

    # No lease duration for publisher and lease duration for subscription
    if pub.liveliness_lease_duration == lease_default and sub.liveliness_lease_duration != lease_default:
        compatibility = QoSCompatibility.ERROR
        return (compatibility, "ERROR: Subscription has a liveliness lease duration, but publisher does not;")

    # Subscription lease duration is less than publisher lease duration
    if pub.liveliness_lease_duration == lease_default and sub.liveliness_lease_duration != lease_default and sub.liveliness_lease_duration < pub.liveliness_lease_duration:
        compatibility = QoSCompatibility.ERROR
        return (compatibility, "ERROR: Subscription liveliness lease duration is less than publisher;")

    pub_reliability_unknown = pub.reliability == QoSReliabilityPolicy.SYSTEM_DEFAULT or pub.reliability == QoSReliabilityPolicy.UNKNOWN
    sub_reliability_unknown = sub.reliability == QoSReliabilityPolicy.SYSTEM_DEFAULT or sub.reliability == QoSReliabilityPolicy.UNKNOWN

    pub_durability_unknown = pub.durability == QoSDurabilityPolicy.SYSTEM_DEFAULT or pub.durability == QoSDurabilityPolicy.UNKNOWN
    sub_durability_unknown = sub.durability == QoSDurabilityPolicy.SYSTEM_DEFAULT or sub.durability == QoSDurabilityPolicy.UNKNOWN

    pub_liveliness_unknown = pub.liveliness == QoSReliabilityPolicy.SYSTEM_DEFAULT or pub.liveliness == QoSReliabilityPolicy.UNKNOWN
    sub_liveliness_unknown = sub.liveliness == QoSReliabilityPolicy.SYSTEM_DEFAULT or sub.liveliness == QoSReliabilityPolicy.UNKNOWN

    # Reliability warnings
    if pub_reliability_unknown and sub_reliability_unknown:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Publisher reliability is {pub_rel} and subscription reliability is {sub_rel};".format(pub_rel=pub.reliability.name, sub_rel=sub.reliability.name))
    elif pub_reliability_unknown and sub.reliability == QoSReliabilityPolicy.RELIABLE:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Reliable subscription, but publisher is {pub_rel};".format(pub_rel=pub.reliability.name))
    elif pub.reliability == QoSReliabilityPolicy.BEST_EFFORT and sub_reliability_unknown:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Best effort publisher, but subscription is {sub_rel};".format(sub_rel=sub.reliability.name))

    # Durability warnings
    if pub_durability_unknown and sub_durability_unknown:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Publisher durabilty is {pub_dur} and subscription durabilty is {sub_dur};".format(pub_dur=pub.durability.name, sub_dur=sub.durability.name))
    elif pub_durability_unknown and sub.durability == QoSDurabilityPolicy.TRANSIENT_LOCAL:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Transient local subscription, but publisher is {pub_dur};".format(pub_dur=pub.durability.name))
    elif pub.durability == QoSDurabilityPolicy.VOLATILE and sub_durability_unknown:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Volatile publisher, but subscription is {sub_dur};".format(sub_dur=sub.durability.name))

    # Liveliness warnings
    if pub_liveliness_unknown and sub_liveliness_unknown:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Publisher liveliness is {pub_liv} and subscription liveliness is {sub_liv};".format(pub_liv=pub.liveliness.name, sub_liv=sub.liveliness.name))
    elif pub_liveliness_unknown and sub.durability == QoSLivelinessPolicy.MANUAL_BY_TOPIC:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Subscription's liveliness is manual by topic, but publisher is {pub_liv};".format(pub_liv=pub.liveliness.name))
    elif pub.durability == QoSLivelinessPolicy.AUTOMATIC and sub_liveliness_unknown:
        compatibility = QoSCompatibility.WARNING
        return (compatibility, "WARNING: Publisher's liveliness is automatic, but subscription is {sub_liv};".format(sub_liv=sub.liveliness.name))

    return (compatibility, "")

class QoSCompatibilityCheck(DoctorCheck):
    """Check for incompatible QoS profiles in each pub/sub pair."""

    def category(self):
        return 'middleware'

    def check(self):
        """Check publisher and subscriber counts."""
        result = Result()
        to_be_checked = get_topic_names()
        with NodeStrategy(None) as node:
            for topic in to_be_checked:
                for pub in node.get_publishers_info_by_topic(topic):
                    for sub in node.get_subscriptions_info_by_topic(topic):
                        compatibility, reason = qos_check_compatible(
                            pub.qos_profile, sub.qos_profile)
                        reason_message = self._strip_leading_warning_or_error_from_string(reason)
                        if compatibility == QoSCompatibility.WARNING:
                            doctor_warn(f"QoS compatibility warning found on topic '{topic}': "
                                        f'{reason_message}')
                            result.add_warning()
                        elif compatibility == QoSCompatibility.ERROR:
                            doctor_error(f"QoS compatibility error found on topic '{topic}': "
                                         f'{reason_message}')
                            result.add_error()
        return result

    @staticmethod
    def _strip_leading_warning_or_error_from_string(string: str) -> str:
        """
        Remove "warning: " or "error: " (case insensitive) from the beginning of a string.

        If "warning: " or "error: " is not found, the original string is returned.
        """
        re_result = re.search(r'^(?i:warning|error): (.*)', string)
        if re_result:
            assert len(re_result.groups()) == 1
            return re_result.groups()[0]
        else:
            return string


class QoSCompatibilityReport(DoctorReport):
    """Report QoS compatibility related information."""

    def category(self):
        return 'middleware'

    def report(self):
        report = Report('QOS COMPATIBILITY LIST')
        to_be_reported = get_topic_names()
        with NodeStrategy(None) as node:
            for topic in to_be_reported:
                for pub in node.get_publishers_info_by_topic(topic):
                    for sub in node.get_subscriptions_info_by_topic(topic):
                        compatibility, reason = qos_check_compatible(
                            pub.qos_profile, sub.qos_profile)
                        report.add_to_report('topic [type]', f'{topic} [{pub.topic_type}]')
                        report.add_to_report('publisher node', pub.node_name)
                        report.add_to_report('subscriber node', sub.node_name)
                        if compatibility == QoSCompatibility.OK:
                            compatibility_msg = 'OK'
                        else:
                            compatibility_msg = reason
                        report.add_to_report('compatibility status', compatibility_msg)
        if self._is_report_empty(report):
            report.add_to_report('compatibility status', 'No publisher/subscriber pairs found')
        return report

    @staticmethod
    def _is_report_empty(report: Report) -> bool:
        return len(report.items) == 0
