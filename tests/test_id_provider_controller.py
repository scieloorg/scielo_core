from unittest import TestCase
from unittest.mock import patch, Mock, ANY

from scielo_core.id_provider import controller


"""
request_document_ids(
        v2, v3, aop_pid,
        doi_with_lang,
        issns,
        pub_year,
        volume, number, suppl,
        elocation_id, fpage, fpage_seq, lpage,
        authors, collab,
        article_titles,
        partial_body,
        xml,
        extra=None,
        user=None)
"""


def _get_registered_document(v3=None, v2=None, aop_pid=None, volume=None):
    record = controller.models.Package()
    if v3:
        record.v3 = v3
    if v2:
        record.v2 = v2
    if aop_pid:
        record.aop_pid = aop_pid
    if volume:
        record.volume = volume

    return record


XML_1_input = ("""<article>
    <front>
    <article-meta>
      <article-id pub-id-type="publisher-id">2237-6089-2020-0149</article-id>
      <article-id pub-id-type="doi">10.47626/2237-6089-2020-0149</article-id>
      <article-id pub-id-type="other">00300</article-id>
    </article-meta>
    </front>
    </article>""")

XML_2_input = ("""<article>
    <front>
    <article-meta>"""
      """<article-id pub-id-type="publisher-id" specific-use="scielo-v3">11111111111111111111111</article-id>"""
      """<article-id pub-id-type="publisher-id" specific-use="scielo-v2">S1234-98762022777777777</article-id>"""
      """<article-id pub-id-type="publisher-id">2237-6089-2020-0149</article-id>
      <article-id pub-id-type="doi">10.47626/2237-6089-2020-0149</article-id>
      <article-id pub-id-type="other">00300</article-id>
    </article-meta>
    </front>
    </article>""")

XML_3_input = ("""<article>
    <front>
    <article-meta>"""
      """<article-id pub-id-type="publisher-id" specific-use="scielo-v3">11111111111111111111111</article-id>"""
      """<article-id pub-id-type="publisher-id" specific-use="scielo-v2">xxxxxxxxx</article-id>"""
      """<article-id pub-id-type="publisher-id">2237-6089-2020-0149</article-id>
      <article-id pub-id-type="doi">10.47626/2237-6089-2020-0149</article-id>
      <article-id pub-id-type="other">00300</article-id>
    </article-meta>
    </front>
    </article>""")


XML_4_input = ("""<article>
    <front>
    <article-meta>"""
      """<article-id pub-id-type="publisher-id" specific-use="scielo-v3">x</article-id>"""
      """<article-id pub-id-type="publisher-id" specific-use="scielo-v2">x</article-id>"""
      """<article-id pub-id-type="publisher-id">2237-6089-2020-0149</article-id>
      <article-id pub-id-type="doi">10.47626/2237-6089-2020-0149</article-id>
      <article-id pub-id-type="other">00300</article-id>
    </article-meta>
    </front>
    </article>""")


class TestFirstRecordWithoutV2andWithoutV3GeneratesV2andV3(TestCase):

    def setUp(self):
        self.args = dict(
            v2=None,
            v3=None,
            aop_pid=None,
            doi_with_lang=None,
            issns=[
                {"type": "epub", "value": "1234-9876"}
            ],
            pub_year='2022',
            volume=None,
            number=None,
            suppl=None,
            elocation_id=None,
            fpage=None,
            fpage_seq=None,
            lpage=None,
            authors=[{
                "surname": "Silva",
                "given_names": "AM",
                "prefix": "Dr",
                "suffix": "Jr",
                "orcid": "9999-9999-9999-9999"}],
            collab=None,
            article_titles=[{
                "lang": "en", "text": "This is an article"
            }],
            partial_body='',
            xml=XML_1_input,
            extra=None,
            user='teste',
        )

    @patch("scielo_core.id_provider.controller._generate_v2_suffix")
    @patch("scielo_core.id_provider.controller._is_registered_v2")
    @patch("scielo_core.id_provider.controller.v3_gen.generates")
    @patch("scielo_core.id_provider.controller._is_registered_v3")
    @patch("scielo_core.id_provider.controller._log_request_update")
    @patch("scielo_core.id_provider.controller._log_new_request")
    @patch("scielo_core.id_provider.controller._register_document")
    @patch("scielo_core.id_provider.controller._get_registered_document")
    def test_task_request_id(
            self,
            mock__get_registered_document,
            mock__register_document,
            mock__log_new_request,
            mock__log_request_update,
            mock__is_registered_v3,
            mock_v3_gen_generates,
            mock__is_registered_v2,
            mock__generate_v2_suffix,
            ):
        mock__get_registered_document.side_effect = (
            controller.exceptions.DocumentDoesNotExistError
        )
        mock__is_registered_v3.return_value = False
        mock_v3_gen_generates.return_value = '1' * 23
        mock__is_registered_v2.return_value = False
        mock__generate_v2_suffix.return_value = '7' * 17

        result = controller.request_document_ids(**self.args)
        self.assertNotIn('previous-pid', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v3">'
            '11111111111111111111111</article-id>', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v2">'
            'S1234-98762022777777777</article-id>', result)

    @patch("scielo_core.id_provider.controller._get_document_omiting_issue_data")
    @patch("scielo_core.id_provider.controller._get_document_published_in_an_issue")
    @patch("scielo_core.id_provider.controller._get_document_published_as_aop")
    def test_get_registered_document(
            self,
            mock__get_document_published_as_aop,
            mock__get_document_published_in_an_issue,
            mock__get_document_omiting_issue_data,
            ):
        mock__get_document_omiting_issue_data.side_effect = [
            None
        ]
        with self.assertRaises(controller.exceptions.DocumentDoesNotExistError):
            result = controller._get_registered_document(self.args)
        mock__get_document_published_as_aop.assert_not_called()
        mock__get_document_published_in_an_issue.assert_not_called()


class TestFirstRecordWithV2andWithV3DoesNotChangeOriginalXML(TestCase):

    def setUp(self):
        self.args = dict(
            v2='S1234-98762022777777777',
            v3='11111111111111111111111',
            aop_pid=None,
            doi_with_lang=None,
            issns=[
                {"type": "epub", "value": "1234-9876"}
            ],
            pub_year='2022',
            volume=None,
            number=None,
            suppl=None,
            elocation_id=None,
            fpage=None,
            fpage_seq=None,
            lpage=None,
            authors=[{
                "surname": "Silva",
                "given_names": "AM",
                "prefix": "Dr",
                "suffix": "Jr",
                "orcid": "9999-9999-9999-9999"}],
            collab=None,
            article_titles=[{
                "lang": "en", "text": "This is an article"
            }],
            partial_body='',
            xml=XML_2_input,
            extra=None,
            user='teste',
        )
        self.registered = None

    @patch("scielo_core.id_provider.controller._generate_v2_suffix")
    @patch("scielo_core.id_provider.controller._is_registered_v2")
    @patch("scielo_core.id_provider.controller.v3_gen.generates")
    @patch("scielo_core.id_provider.controller._is_registered_v3")
    @patch("scielo_core.id_provider.controller._log_request_update")
    @patch("scielo_core.id_provider.controller._log_new_request")
    @patch("scielo_core.id_provider.controller._register_document")
    @patch("scielo_core.id_provider.controller._get_registered_document")
    def test_task_request_id(
            self,
            mock__get_registered_document,
            mock__register_document,
            mock__log_new_request,
            mock__log_request_update,
            mock__is_registered_v3,
            mock_v3_gen_generates,
            mock__is_registered_v2,
            mock__generate_v2_suffix,
            ):
        mock__get_registered_document.side_effect = (
            controller.exceptions.DocumentDoesNotExistError
        )
        mock__is_registered_v3.return_value = False
        mock_v3_gen_generates.return_value = '1' * 23
        mock__is_registered_v2.return_value = False
        mock__generate_v2_suffix.return_value = '7' * 17

        result = controller.request_document_ids(**self.args)
        # nao houve mudança no XML
        self.assertIsNone(result)

    @patch("scielo_core.id_provider.controller._get_document_omiting_issue_data")
    @patch("scielo_core.id_provider.controller._get_document_published_in_an_issue")
    @patch("scielo_core.id_provider.controller._get_document_published_as_aop")
    def test_get_registered_document(
            self,
            mock__get_document_published_as_aop,
            mock__get_document_published_in_an_issue,
            mock__get_document_omiting_issue_data,
            ):
        mock__get_document_omiting_issue_data.side_effect = [
            None
        ]
        with self.assertRaises(controller.exceptions.DocumentDoesNotExistError):
            result = controller._get_registered_document(self.args)
        mock__get_document_published_as_aop.assert_not_called()
        mock__get_document_published_in_an_issue.assert_not_called()


class TestInputWithConflictingV2RecoversRegisteredV2andChangesOriginalXML(TestCase):

    def setUp(self):
        self.args = dict(
            v2='xxxxxxxxx',
            v3='11111111111111111111111',
            aop_pid=None,
            doi_with_lang=None,
            issns=[
                {"type": "epub", "value": "1234-9876"}
            ],
            pub_year='2022',
            volume=None,
            number=None,
            suppl=None,
            elocation_id=None,
            fpage=None,
            fpage_seq=None,
            lpage=None,
            authors=[{
                "surname": "Silva",
                "given_names": "AM",
                "prefix": "Dr",
                "suffix": "Jr",
                "orcid": "9999-9999-9999-9999"}],
            collab=None,
            article_titles=[{
                "lang": "en", "text": "This is an article"
            }],
            partial_body='',
            xml=XML_3_input,
            extra=None,
            user='teste',
        )
        self.registered = _get_registered_document(
            v3='11111111111111111111111',
            v2='S1234-98762022777777777',
            aop_pid=None
        )

    @patch("scielo_core.id_provider.controller._generate_v2_suffix")
    @patch("scielo_core.id_provider.controller._is_registered_v2")
    @patch("scielo_core.id_provider.controller.v3_gen.generates")
    @patch("scielo_core.id_provider.controller._is_registered_v3")
    @patch("scielo_core.id_provider.controller._log_request_update")
    @patch("scielo_core.id_provider.controller._log_new_request")
    @patch("scielo_core.id_provider.controller._register_document")
    @patch("scielo_core.id_provider.controller._get_registered_document")
    def test_task_request_id(
            self,
            mock__get_registered_document,
            mock__register_document,
            mock__log_new_request,
            mock__log_request_update,
            mock__is_registered_v3,
            mock_v3_gen_generates,
            mock__is_registered_v2,
            mock__generate_v2_suffix,
            ):
        mock__get_registered_document.return_value = self.registered
        mock__is_registered_v3.return_value = False
        mock_v3_gen_generates.return_value = '1' * 23
        mock__is_registered_v2.return_value = False
        mock__generate_v2_suffix.return_value = '7' * 17

        result = controller.request_document_ids(**self.args)
        self.assertNotIn('previous-pid', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v3">'
            '11111111111111111111111</article-id>', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v2">'
            'S1234-98762022777777777</article-id>', result)

    @patch("scielo_core.id_provider.controller._fetch_most_recent_document")
    @patch("scielo_core.id_provider.controller._get_document_omiting_issue_data")
    @patch("scielo_core.id_provider.controller._get_document_published_in_an_issue")
    @patch("scielo_core.id_provider.controller._get_document_published_as_aop")
    def test_get_registered_document(
            self,
            mock__get_document_published_as_aop,
            mock__get_document_published_in_an_issue,
            mock__get_document_omiting_issue_data,
            mock__fetch_most_recent_document,
            ):
        mock__get_document_omiting_issue_data.side_effect = [
            self.registered
        ]
        mock__fetch_most_recent_document.return_value = self.registered
        result = controller._get_registered_document(self.args)
        self.assertEqual(self.registered, result)
        mock__get_document_published_as_aop.assert_not_called()
        mock__get_document_published_in_an_issue.assert_not_called()


class TestInputWithConflictingV3RecoversRegisteredV3andChangesOriginalXML(TestCase):

    def setUp(self):
        self.args = dict(
            v2='S1234-98762022777777777',
            v3='xxxxxxxxx',
            aop_pid=None,
            doi_with_lang=None,
            issns=[
                {"type": "epub", "value": "1234-9876"}
            ],
            pub_year='2022',
            volume=None,
            number=None,
            suppl=None,
            elocation_id=None,
            fpage=None,
            fpage_seq=None,
            lpage=None,
            authors=[{
                "surname": "Silva",
                "given_names": "AM",
                "prefix": "Dr",
                "suffix": "Jr",
                "orcid": "9999-9999-9999-9999"}],
            collab=None,
            article_titles=[{
                "lang": "en", "text": "This is an article"
            }],
            partial_body='',
            xml=XML_3_input,
            extra=None,
            user='teste',
        )
        self.registered = _get_registered_document(
            v3='11111111111111111111111',
            v2='S1234-98762022777777777',
            aop_pid=None
        )

    @patch("scielo_core.id_provider.controller._generate_v2_suffix")
    @patch("scielo_core.id_provider.controller._is_registered_v2")
    @patch("scielo_core.id_provider.controller.v3_gen.generates")
    @patch("scielo_core.id_provider.controller._is_registered_v3")
    @patch("scielo_core.id_provider.controller._log_request_update")
    @patch("scielo_core.id_provider.controller._log_new_request")
    @patch("scielo_core.id_provider.controller._register_document")
    @patch("scielo_core.id_provider.controller._get_registered_document")
    def test_task_request_id(
            self,
            mock__get_registered_document,
            mock__register_document,
            mock__log_new_request,
            mock__log_request_update,
            mock__is_registered_v3,
            mock_v3_gen_generates,
            mock__is_registered_v2,
            mock__generate_v2_suffix,
            ):
        mock__get_registered_document.return_value = self.registered
        mock__is_registered_v3.return_value = False
        mock_v3_gen_generates.return_value = '1' * 23
        mock__is_registered_v2.return_value = False
        mock__generate_v2_suffix.return_value = '7' * 17

        result = controller.request_document_ids(**self.args)
        self.assertNotIn('previous-pid', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v3">'
            '11111111111111111111111</article-id>', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v2">'
            'S1234-98762022777777777</article-id>', result)

    @patch("scielo_core.id_provider.controller._fetch_most_recent_document")
    @patch("scielo_core.id_provider.controller._get_document_omiting_issue_data")
    @patch("scielo_core.id_provider.controller._get_document_published_in_an_issue")
    @patch("scielo_core.id_provider.controller._get_document_published_as_aop")
    def test_get_registered_document(
            self,
            mock__get_document_published_as_aop,
            mock__get_document_published_in_an_issue,
            mock__get_document_omiting_issue_data,
            mock__fetch_most_recent_document,
            ):
        mock__get_document_omiting_issue_data.side_effect = [
            self.registered
        ]
        mock__fetch_most_recent_document.return_value = self.registered
        result = controller._get_registered_document(self.args)
        self.assertEqual(self.registered, result)
        mock__get_document_published_in_an_issue.assert_not_called()
        mock__get_document_published_as_aop.assert_not_called()


class TestInputHasRegisteredAOPVersionRecoversPreviousV2andChangesOriginalXML(TestCase):

    def setUp(self):
        self.args = dict(
            v2='S1234-98762022777777777',
            v3='11111111111111111111111',
            aop_pid=None,
            doi_with_lang=None,
            issns=[
                {"type": "epub", "value": "1234-9876"}
            ],
            pub_year='2022',
            volume='44',
            number=None,
            suppl=None,
            elocation_id=None,
            fpage=None,
            fpage_seq=None,
            lpage=None,
            authors=[{
                "surname": "Silva",
                "given_names": "AM",
                "prefix": "Dr",
                "suffix": "Jr",
                "orcid": "9999-9999-9999-9999"}],
            collab=None,
            article_titles=[{
                "lang": "en", "text": "This is an article"
            }],
            partial_body='',
            xml=XML_3_input,
            extra=None,
            user='teste',
        )

        self.registered = _get_registered_document(
            v3='11111111111111111111111',
            v2='S1234-98762022005055555',
            aop_pid=None
        )

    @patch("scielo_core.id_provider.controller._generate_v2_suffix")
    @patch("scielo_core.id_provider.controller._is_registered_v2")
    @patch("scielo_core.id_provider.controller.v3_gen.generates")
    @patch("scielo_core.id_provider.controller._is_registered_v3")
    @patch("scielo_core.id_provider.controller._log_request_update")
    @patch("scielo_core.id_provider.controller._log_new_request")
    @patch("scielo_core.id_provider.controller._register_document")
    @patch("scielo_core.id_provider.controller._get_registered_document")
    def test_task_request_id(
            self,
            mock__get_registered_document,
            mock__register_document,
            mock__log_new_request,
            mock__log_request_update,
            mock__is_registered_v3,
            mock_v3_gen_generates,
            mock__is_registered_v2,
            mock__generate_v2_suffix,
            ):
        mock__get_registered_document.return_value = self.registered
        mock__is_registered_v3.return_value = False
        mock_v3_gen_generates.return_value = '1' * 23
        mock__is_registered_v2.return_value = False
        mock__generate_v2_suffix.return_value = '7' * 17

        result = controller.request_document_ids(**self.args)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="previous-pid">'
            'S1234-98762022005055555</article-id>', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v3">'
            '11111111111111111111111</article-id>', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v2">'
            'S1234-98762022777777777</article-id>', result)

    @patch("scielo_core.id_provider.controller._fetch_most_recent_document")
    @patch("scielo_core.id_provider.controller._get_document_omiting_issue_data")
    @patch("scielo_core.id_provider.controller._get_document_published_in_an_issue")
    @patch("scielo_core.id_provider.controller._get_document_published_as_aop")
    def test_get_registered_document(
            self,
            mock__get_document_published_as_aop,
            mock__get_document_published_in_an_issue,
            mock__get_document_omiting_issue_data,
            mock__fetch_most_recent_document,
            ):

        mock__get_document_published_in_an_issue.side_effect = [
            None,
            None,
        ]
        mock__get_document_published_as_aop.return_value = self.registered
        mock__fetch_most_recent_document.return_value = self.registered
        result = controller._get_registered_document(self.args)
        self.assertEqual(self.registered, result)
        mock__get_document_omiting_issue_data.assert_not_called()


class TestInputWithPidsRecoversRegisteredPidsandChangesOriginalXML(TestCase):

    def setUp(self):
        self.args = dict(
            v2='x',
            v3='x',
            aop_pid='x',
            doi_with_lang=None,
            issns=[
                {"type": "epub", "value": "1234-9876"}
            ],
            pub_year='2022',
            volume='44',
            number=None,
            suppl=None,
            elocation_id=None,
            fpage=None,
            fpage_seq=None,
            lpage=None,
            authors=[{
                "surname": "Silva",
                "given_names": "AM",
                "prefix": "Dr",
                "suffix": "Jr",
                "orcid": "9999-9999-9999-9999"}],
            collab=None,
            article_titles=[{
                "lang": "en", "text": "This is an article"
            }],
            partial_body='',
            xml=XML_3_input,
            extra=None,
            user='teste',
        )
        self.registered = _get_registered_document(
            v3='11111111111111111111111',
            v2='S1234-98762022777777777',
            aop_pid='S1234-98762022505050555',
            volume='44'
        )

    @patch("scielo_core.id_provider.controller._generate_v2_suffix")
    @patch("scielo_core.id_provider.controller._is_registered_v2")
    @patch("scielo_core.id_provider.controller.v3_gen.generates")
    @patch("scielo_core.id_provider.controller._is_registered_v3")
    @patch("scielo_core.id_provider.controller._log_request_update")
    @patch("scielo_core.id_provider.controller._log_new_request")
    @patch("scielo_core.id_provider.controller._register_document")
    @patch("scielo_core.id_provider.controller._get_registered_document")
    def test_task_request_id(
            self,
            mock__get_registered_document,
            mock__register_document,
            mock__log_new_request,
            mock__log_request_update,
            mock__is_registered_v3,
            mock_v3_gen_generates,
            mock__is_registered_v2,
            mock__generate_v2_suffix,
            ):
        mock__get_registered_document.return_value = self.registered
        mock__is_registered_v3.return_value = False
        mock_v3_gen_generates.return_value = '1' * 23
        mock__is_registered_v2.return_value = False
        mock__generate_v2_suffix.return_value = '7' * 17

        result = controller.request_document_ids(**self.args)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="previous-pid">'
            'S1234-98762022505050555</article-id>', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v3">'
            '11111111111111111111111</article-id>', result)
        self.assertIn(
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v2">'
            'S1234-98762022777777777</article-id>', result)

    @patch("scielo_core.id_provider.controller._fetch_most_recent_document")
    @patch("scielo_core.id_provider.controller._get_document_omiting_issue_data")
    @patch("scielo_core.id_provider.controller._get_document_published_in_an_issue")
    @patch("scielo_core.id_provider.controller._get_document_published_as_aop")
    def test_get_registered_document(
            self,
            mock__get_document_published_as_aop,
            mock__get_document_published_in_an_issue,
            mock__get_document_omiting_issue_data,
            mock__fetch_most_recent_document,
            ):

        mock__get_document_published_in_an_issue.side_effect = [
            None,
            self.registered
        ]
        mock__fetch_most_recent_document.return_value = self.registered

        result = controller._get_registered_document(self.args)
        mock__get_document_omiting_issue_data.assert_not_called()
        mock__get_document_published_as_aop.assert_not_called()
        self.assertEqual(self.registered, result)


class TestInputWithAOPDataIsRejectedBecauseRegisteredDocIsExAOP(TestCase):

    def setUp(self):
        self.args = dict(
                v2='x',
                v3='x',
                aop_pid=None,
                doi_with_lang=None,
                issns=[
                    {"type": "epub", "value": "1234-9876"}
                ],
                pub_year='2022',
                volume=None,
                number=None,
                suppl=None,
                elocation_id=None,
                fpage=None,
                fpage_seq=None,
                lpage=None,
                authors=[{
                    "surname": "Silva",
                    "given_names": "AM",
                    "prefix": "Dr",
                    "suffix": "Jr",
                    "orcid": "9999-9999-9999-9999"}],
                collab=None,
                article_titles=[{
                    "lang": "en", "text": "This is an article"
                }],
                partial_body='',
                xml=XML_4_input,
                extra=None,
                user='teste',
            )
        self.registered = _get_registered_document(
            v3='11111111111111111111111',
            v2='S1234-98762022777777777',
            aop_pid='S1234-98762022505050555',
            volume='44'
        )

    @patch("scielo_core.id_provider.controller._generate_v2_suffix")
    @patch("scielo_core.id_provider.controller._is_registered_v2")
    @patch("scielo_core.id_provider.controller.v3_gen.generates")
    @patch("scielo_core.id_provider.controller._is_registered_v3")
    @patch("scielo_core.id_provider.controller._log_request_update")
    @patch("scielo_core.id_provider.controller._log_new_request")
    @patch("scielo_core.id_provider.controller._register_document")
    @patch("scielo_core.id_provider.controller._get_registered_document")
    def test_task_request_id(
            self,
            mock__get_registered_document,
            mock__register_document,
            mock__log_new_request,
            mock__log_request_update,
            mock__is_registered_v3,
            mock_v3_gen_generates,
            mock__is_registered_v2,
            mock__generate_v2_suffix,
            ):
        mock__get_registered_document.return_value = self.registered
        mock__is_registered_v3.return_value = False
        mock_v3_gen_generates.return_value = '1' * 23
        mock__is_registered_v2.return_value = False
        mock__generate_v2_suffix.return_value = '7' * 17

        with self.assertRaises(controller.exceptions.NotAllowedAOPInputError):
            controller.request_document_ids(**self.args)

    @patch("scielo_core.id_provider.controller._fetch_most_recent_document")
    @patch("scielo_core.id_provider.controller._get_document_omiting_issue_data")
    @patch("scielo_core.id_provider.controller._get_document_published_in_an_issue")
    @patch("scielo_core.id_provider.controller._get_document_published_as_aop")
    def test_get_registered_document(
            self,
            mock__get_document_published_as_aop,
            mock__get_document_published_in_an_issue,
            mock__get_document_omiting_issue_data,
            mock__fetch_most_recent_document,
            ):

        mock__get_document_omiting_issue_data.side_effect = [
            self.registered
        ]
        mock__fetch_most_recent_document.return_value = self.registered

        result = controller._get_registered_document(self.args)
        mock__get_document_published_in_an_issue.assert_not_called()
        mock__get_document_published_as_aop.assert_not_called()
        self.assertEqual(self.registered, result)


class TestInputWithPidsRecoversRegisteredPidsandDoesNotChangeOriginalXML(TestCase):

    def setUp(self):
        self.args = dict(
            v2='S1234-98762022777777777',
            v3='11111111111111111111111',
            aop_pid='S1234-98762022505050555',
            doi_with_lang=None,
            issns=[
                {"type": "epub", "value": "1234-9876"}
            ],
            pub_year='2022',
            volume='44',
            number=None,
            suppl=None,
            elocation_id=None,
            fpage=None,
            fpage_seq=None,
            lpage=None,
            authors=[{
                "surname": "Silva",
                "given_names": "AM",
                "prefix": "Dr",
                "suffix": "Jr",
                "orcid": "9999-9999-9999-9999"}],
            collab=None,
            article_titles=[{
                "lang": "en", "text": "This is an article"
            }],
            partial_body='',
            xml=XML_3_input,
            extra=None,
            user='teste',
        )
        self.registered = _get_registered_document(
            v3='11111111111111111111111',
            v2='S1234-98762022777777777',
            aop_pid='S1234-98762022505050555',
            volume='44'
        )

    @patch("scielo_core.id_provider.controller._generate_v2_suffix")
    @patch("scielo_core.id_provider.controller._is_registered_v2")
    @patch("scielo_core.id_provider.controller.v3_gen.generates")
    @patch("scielo_core.id_provider.controller._is_registered_v3")
    @patch("scielo_core.id_provider.controller._log_request_update")
    @patch("scielo_core.id_provider.controller._log_new_request")
    @patch("scielo_core.id_provider.controller._register_document")
    @patch("scielo_core.id_provider.controller._get_registered_document")
    def test_task_request_id(
            self,
            mock__get_registered_document,
            mock__register_document,
            mock__log_new_request,
            mock__log_request_update,
            mock__is_registered_v3,
            mock_v3_gen_generates,
            mock__is_registered_v2,
            mock__generate_v2_suffix,
            ):
        mock__get_registered_document.return_value = self.registered
        mock__is_registered_v3.return_value = False
        mock_v3_gen_generates.return_value = '1' * 23
        mock__is_registered_v2.return_value = False
        mock__generate_v2_suffix.return_value = '7' * 17

        result = controller.request_document_ids(**self.args)
        self.assertIsNone(result)

    @patch("scielo_core.id_provider.controller._fetch_most_recent_document")
    @patch("scielo_core.id_provider.controller._get_document_omiting_issue_data")
    @patch("scielo_core.id_provider.controller._get_document_published_in_an_issue")
    @patch("scielo_core.id_provider.controller._get_document_published_as_aop")
    def test_get_registered_document(
            self,
            mock__get_document_published_as_aop,
            mock__get_document_published_in_an_issue,
            mock__get_document_omiting_issue_data,
            mock__fetch_most_recent_document,
            ):

        mock__get_document_published_in_an_issue.side_effect = [
            self.registered
        ]
        mock__fetch_most_recent_document.return_value = self.registered

        result = controller._get_registered_document(self.args)
        mock__get_document_omiting_issue_data.assert_not_called()
        mock__get_document_published_as_aop.assert_not_called()
        self.assertEqual(self.registered, result)
