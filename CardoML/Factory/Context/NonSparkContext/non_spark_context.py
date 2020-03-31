import logging
import logging.config
import uuid
from logging import Logger

from CardoExecutor.Common.Factory.CardoContextFactoryConf import CardoContextFactoryConf
from CardoExecutor.Common.Factory.Templates.elastic_logging_config_template import get_config as get_logging_config
from CardoExecutor.Contract.CardoContextBase import CardoContextBase

import requests
from CardoExecutor.Common.Factory.Templates.elastic_logging_config_template import KIBANA_RUN_ID_URL, \
    KIBANA_SHORTEN_SERVICE, \
    KIBANA_SHORTEN_SERVICE_HEADER, KIBANA_SHORTEN_RESULT_TEMPLATE


class NonSparkContextFactory(object):
    """
    Make cardo work without using a spark context.
    """
    def create_context(self, app_name, environment="your_env1", append_log_config=None, logger=None, cluster="your_env2"):
        # type: (str, str, dict, Logger, str) -> NonSparkContext
        context_conf = CardoContextFactoryConf(**{key: value for key, value in locals().items() if key != 'self'})
        append_log_config = append_log_config or dict()
        context_conf.run_id = str(uuid.uuid1()).lower()
        self.__logger = CardoContextFactoryLoggings()
        context = self.__create_context(context_conf)
        return context

    def __create_context(self, context_conf):
        log_config = self.__get_log_config(context_conf)
        context = NonSparkContext(log_config=log_config,
                                  logger=context_conf.logger, run_id=context_conf.run_id)
        self.__logger.log_about_creation(context)
        return context

    def __get_log_config(self, context_conf):
        # type: (CardoContextFactoryConf) -> dict
        logging_level = "NOTSET" if context_conf.environment == "dev" else "INFO"
        log_config = get_logging_config(environment=context_conf.environment, app_name=context_conf.app_name,
                                        level=logging_level,
                                        run_id=context_conf.run_id)
        log_config.update(context_conf.append_log_config or {})
        return log_config


class NonSparkContext(CardoContextBase):
    def __init__(self, log_config, run_id=None, logger=None):
        # type: (dict, str, Logger) -> None
        super(NonSparkContext, self).__init__()
        self._set_logger(log_config, logger)
        self.run_id = run_id or uuid.uuid1()

    def _set_logger(self, log_config, logger):
        if logger is None:
            logging.config.dictConfig(log_config)
            self.logger = logging.getLogger('main')
        else:
            self.logger = logger


class CardoContextFactoryLoggings(object):

    def log_about_creation(self, context):
        self.__log_kibana_location(context)

    def __log_kibana_location(self, context):
        try:
            kibana_full_url = KIBANA_RUN_ID_URL.format(run_id=context.run_id)
            kibana_shorten_url = requests.post(KIBANA_SHORTEN_SERVICE, headers=KIBANA_SHORTEN_SERVICE_HEADER,
                                               json={'url': kibana_full_url}).text
            context.logger.info(
                'Kibana logs: {url}'.format(url=KIBANA_SHORTEN_RESULT_TEMPLATE.format(kibana_shorten_url)))
        except Exception as e:
            context.logger.warn('Cannot link kibana', exec_info=e)
