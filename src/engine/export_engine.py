# from engine.source import RulesEng, FunctionResult
from .rules_engine import RulesEng, FunctionResult  
import pandas as pd
from abc import ABC, abstractmethod


class ExportEng():
    def __init__(self, df:pd.DataFrame) -> None:
        self.df = df
        self.rules = self.set_content_rules()
        self.rules_executed = False
        
    @abstractmethod
    def set_content_rules(self) -> RulesEng:...

    def execute_rules(self):     
        while self.rules.has_rules:
            self.rules.execute_rule()
        self.rules_executed = self.rules.rules_executed

    def save_result(self, path:str, join:bool=False):
        result:FunctionResult
        if not self.rules.rules_has_execute:
            raise Exception("[ExportEng>save_result: ERROR] Rules still isn't executed")
        
        if join:
            df = pd.DataFrame()
            for rule_name, result in self.rules.export_rules_result():
                if rule_name in self.rules.export:
                    df = pd.concat([df, result.get_result()])
            df.to_excel(path)
        else:
            writer = pd.ExcelWriter(path, engine='openpyxl')
            for rule_name, result in self.rules.export_rules_result():
                if rule_name in self.rules.export:
                    result.get_result().to_excel(writer, sheet_name=rule_name)
            writer.close()
            return 
    
    def save_spare(self, path:str):
        result:FunctionResult
        if not self.rules.rules_has_execute:
            raise Exception("[ExportEng>save_result: ERROR] Rules still isn't executed")
        
        writer = pd.ExcelWriter(path, engine='openpyxl')
        for rule_name, result in self.rules.export_rules_result():
            result.get_spare().to_excel(writer, sheet_name=rule_name)
        writer.close()


            


                
            

    

