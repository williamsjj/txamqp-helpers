#!/usr/bin/python
####################################################################
# FILENAME: setup.py
# PROJECT: txAMQP Helpers
# DESCRIPTION: Utility classes for making txAMQP easier to work 
#              with. Code from Dan Siemon...just packaged into 
#              a Python package.
# 
#   License & Attribution:
#
# Dan Siemon <dan@coverfire.com>
# March 2010
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
####################################################################

from setuptools import setup, find_packages
 
version = '0.5'
 
setup(name='txamqp-helpers',
      version=version,
      description="txAMQP Helpers",
      long_description="""Utility classes for making txAMQP easier to work with.""",
      classifiers=[],
      keywords='',
      author='Jason Williams/Dan Siemon',
      author_email='jasonjwwilliams@gmail.com',
      url='https://github.com/williamsjj/txamqp-helpers',
      download_url='https://github.com/williamsjj/txamqp-helpers/zipball/v0.5',
      license='Apache 2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests', 'old*']),
      zip_safe=False,
      install_requires=["Twisted>=10.0",
                        "txAMQP>=0.4"]
    )