import pandas as pd


class FunctionResult():
    def __init__(self, result, spare) -> None:
        self.result = result
        self.spare = spare

    def get_result(self) -> pd.DataFrame:
        return self.result

    def get_spare(self) -> pd.DataFrame:
        return self.spare

    def set_result(self, result) -> None:
        self.result = result
        
    def set_spare(self, spare) -> None:
        self.spare = spare


class RulesEng():
    def __init__(self, df: pd.DataFrame) -> None:
        self.rules: list = []
        self.rules_executed = {}
        self.has_rules = True
        self.df = df
        self.export = []

    def add_rule(self, name: str, function, get: str = 'last', get_result:bool = False, export:bool=True):
        """
        ## add_rule
        :param get: where will catch the DataFrame:
            -> last: DataFrame Generated in last rule
            -> False: self.df
        :param get_result: Will get the result from the "get" rule name
        """
        self.rules.append((name, {'function': function, 'get': get, 'get_result': get_result, 'export':export}))

    def execute_rule(self) -> None:
        if not self.has_rules:
            raise (Exception("[RulesEng>execute_rule: ERROR]Has no rules"))

        rule_to_execute = self.rules.pop(0)

        if self.rules == []:
            self.has_rules = False
            self.rules_has_execute = True

        name_rule = rule_to_execute[0]
        parameters = rule_to_execute[1]
        function = parameters['function']
        get = parameters['get']
        get_result = parameters['get_result']

        if parameters['export']:
            self.export.append(name_rule)

        if get:
            if get == 'last':
                if not self.rules_executed:
                    df_to_use = self.df
                else:
                    to_use: FunctionResult = self.rules_executed[list(
                        self.rules_executed.keys())[-1]]
                    df_to_use = to_use.get_spare()

            elif get not in self.rules_executed:
                raise (Exception(
                    f"[RuleContent>__getitem__: ERROR]'{get}' has not been executed yet or not seted"))

            else:
                if get_result:
                    df_to_use = self.rules_executed[get].get_result()
                else:
                    df_to_use = self.rules_executed[get].get_spare()
        else:
            df_to_use = self.df

        df_result = function(df_to_use)
        self.rules_executed[name_rule] = df_result

    def export_rules_result(self) -> tuple:
        return tuple(self.rules_executed.items())
