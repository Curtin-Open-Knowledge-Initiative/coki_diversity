# Copyright 2021 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Cameron Neylon

"""Filters for Diversity Data Normalisation

Some documentation goes here describing the outline process

Any source must provide at minimum a filter that provides a total academic_total_count (which may actually be FTE)
and an academic_women_count (similarly). If this is not feasible then changes will need to be made in the source
__init__ file to avoid errors and this should be noted in the main documentation as an exception.

"""

from ..generic import FileFilter

specific_filter_startyr_endyr = FileFilter(source='source_name',  # Must correspond to name of source package
                                           years=range(2010, 2020),  # Note that range is non-inclusive for the
                                           # final year so this would cover 2010-2019
                                           reqs={'category_name': ['values', 'to', 'include'],
                                                     'another_category': ['All']  # all values of this category should
                                                                                  # be combined
                                                     }
                                           )

academic_total_count = [] # Add relevant filters to this list
academic_women_count = [] # Add relevant filters to this list

if (len(academic_total_count) == 0) or (len(academic_women_count) == 0):
    raise NotImplementedError('Implementation of academic_total_count and academic_women_count is required')
