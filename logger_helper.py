# -*- coding: utf-8 -*-
# =============================================================================
#     FileName:
#         Desc:
#       Author:
#        Email:
#     HomePage:
#      Version:
#   LastChange:
#      History:
# =============================================================================


import logging, os
import logging.config


class LoggerHelper(object):
    @staticmethod
    def get_logger(logger_name=None):
        if logger_name is None:
            logger_name = 'default'
        return logging.getLogger(logger_name)

    @staticmethod
    def get_logging_config():
        site_dir = os.path.dirname(__file__)
        logger_dir = os.path.join(site_dir, 'logs')
        if not os.path.exists(logger_dir):
            os.makedirs(logger_dir)
        config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard_formatter': {
                    'format': '%(asctime)s [%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s'
                }
            },
            'handlers': {
                'default_handler': {
                    'level': 'DEBUG',
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': os.path.join(logger_dir, 'ha.log'),
                    'maxBytes': 1024 * 1024 * 500,
                    'backupCount': 20,
                    'formatter': 'standard_formatter',
                },
                'mysql_server_handler': {
                    'level': 'DEBUG',
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': os.path.join(logger_dir, 'mysql_server.log'),
                    'maxBytes': 1024 * 1024 * 500,
                    'backupCount': 20,
                    'formatter': 'standard_formatter',
                },
                'stream_handler': {
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'standard_formatter',
                },

            },
            'loggers': {
                'default': {
                    'handlers': ['default_handler', 'stream_handler'],
                    'level': 'INFO',
                    'propagate': False,
                },
                'mysql_server': {
                    'handlers': ['mysql_server_handler'],
                    'level': 'INFO',
                    'propagate': False,
                },
            }
        }
        return config

    @staticmethod
    def init_logging():
        print("init logging config")
        logging_config = LoggerHelper.get_logging_config()
        logging.config.dictConfig(logging_config)
