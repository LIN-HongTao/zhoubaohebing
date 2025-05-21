import pandas as pd
import sys
import os
from PyQt6.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, 
                           QHBoxLayout, QFileDialog, QLabel, QWidget, QTabWidget, QTableView,
                           QHeaderView, QMessageBox, QLineEdit, QFormLayout, QInputDialog, QDialog)
from PyQt6.QtCore import Qt, QAbstractTableModel
from typing import Dict, List, Optional, Union, Any, Callable
import re

class PandasModel(QAbstractTableModel):
    """用于在QTableView中显示Pandas DataFrame的模型类"""
    
    def __init__(self, data: pd.DataFrame):
        super().__init__()
        self._data = data

    def rowCount(self, parent: Optional[Any] = None) -> int:
        return len(self._data)

    def columnCount(self, parent: Optional[Any] = None) -> int:
        return len(self._data.columns)

    def data(self, index: Any, role: int = Qt.ItemDataRole.DisplayRole) -> Optional[str]:
        if not index.isValid():
            return None
            
        if role == Qt.ItemDataRole.DisplayRole:
            value = self._data.iloc[index.row(), index.column()]
            # 处理不同类型的数据
            if pd.isna(value):
                return ""
            return str(value)
            
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, 
                  role: int = Qt.ItemDataRole.DisplayRole) -> Optional[str]:
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._data.columns[section])
            if orientation == Qt.Orientation.Vertical:
                return str(self._data.index[section])
        return None


class ExcelProcessor:
    """Excel处理类，用于读取和处理Excel文件中的表格数据"""
    
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        self.tables: Dict[str, pd.DataFrame] = {}
        self.processed_tables: Dict[str, pd.DataFrame] = {}
        self.deposit_tables: Dict[str, pd.DataFrame] = {}  # 新增：用于存储保证金相关表格
        self.future_tables: Dict[str, pd.DataFrame] = {}  # 用于存储未定价或远期交货业务表格
        
    def set_file_path(self, file_path: str) -> None:
        """设置Excel文件路径"""
        self.file_path = file_path
        
    def read_excel(self) -> bool:
        """读取Excel文件中的'贸易经营风险指标'表"""
        if not self.file_path:
            return False
            
        try:
            # 读取整个工作表
            print(f"正在读取文件: {self.file_path}")
            sheet_data = pd.read_excel(self.file_path, sheet_name="贸易经营风险指标", header=None)
            
            # 表格名称及其正则表达式模式
            table_patterns = [
                (r"一、\s*逾期还款业务", "一、逾期还款业务"),
                (r"二、\s*付款逾期未到货[\(（]1[\)）]", "二、付款逾期未到货(1)"),
                (r"三、\s*付款逾期未到货[\(（]2、集港及在途部分[\)）]", "三、付款逾期未到货(2、集港及在途部分)"),
                (r"四、\s*转口销售逾期未开证", "四、转口销售逾期未开证"),
                (r"五、\s*签约未到货", "五、签约未到货"),
                (r"六、\s*逾期未交货[/／]未验收[/／]未退质保金[/／]未结算", "六、逾期未交货/未验收/未退质保金/未结算"),
                (r"七、\s*投标保证金逾期退还表", "七、投标保证金逾期退还表"),
                (r"八、\s*现货敞口90天及以上库存", "八、现货敞口90天及以上库存"),
                (r"九、\s*期现结合90天及以上库存", "九、期现结合90天及以上库存")
            ]
            
            # 查找每个表格的开始位置
            start_rows = []
            for pattern, name in table_patterns:
                found = False
                for idx, row in sheet_data.iterrows():
                    cell_value = str(row[0]) if not pd.isna(row[0]) else ""
                    if re.search(pattern, cell_value):
                        print(f"找到表格标题: {cell_value} 在行 {idx}")
                        # 直接使用Python的整数索引
                        try:
                            # 确保idx是整数
                            idx_int = int(idx) if not isinstance(idx, int) else idx
                            start_row = idx_int + 1
                            start_rows.append((start_row, name))
                            found = True
                            break
                        except (ValueError, TypeError):
                            print(f"警告: 行索引转换为整数失败: {idx}，类型: {type(idx)}")
                            continue
                if not found:
                    print(f"警告: 未找到表格 '{name}'")
            
            # 按行号排序表格起始位置
            start_rows.sort(key=lambda x: x[0])
            
            # 计算每个表格的结束位置
            for i in range(len(start_rows)):
                start_row, table_name = start_rows[i]
                
                # 如果不是最后一个表格，则下一个表格的开始行是当前表格的结束行
                if i < len(start_rows) - 1:
                    end_row = start_rows[i + 1][0] - 1
                else:
                    # 对于最后一个表格，查找连续的空行作为结束标志
                    end_row = self._find_table_end(sheet_data, start_row)
                
                # 提取表格数据，确保至少包含两行（标题行和至少一行数据）
                if end_row - start_row < 1:
                    print(f"警告: 表格 '{table_name}' 太短，可能无效")
                    continue
                
                table_data = sheet_data.iloc[start_row:end_row+1].copy()
                
                # 处理标题行，设置为DataFrame的列名
                headers = table_data.iloc[0].fillna('')
                table_data = table_data.iloc[1:].copy()
                table_data.columns = headers
                
                # 清除空行
                table_data = table_data.dropna(how='all')
                
                # 存储表格数据
                self.tables[table_name] = table_data
                print(f"成功读取表格: {table_name}, 行数: {len(table_data)}")
                
            return True
            
        except Exception as e:
            print(f"读取Excel文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
            
    def read_deposit_sheet(self) -> bool:
        """读取Excel文件中的'保证金'sheet页"""
        if not self.file_path:
            return False
            
        try:
            # 读取保证金工作表
            print(f"正在读取保证金sheet: {self.file_path}")
            sheet_data = pd.read_excel(self.file_path, sheet_name="保证金", header=None)
            
            # 表格名称及其正则表达式模式 - 已处理兼容全角和半角符号
            table_patterns = [
                (r"保证金比例低于合同约定比例", "保证金比例低于合同约定比例"),
                (r"未约定收保证金的锁定业务价格倒挂情况", "未约定收保证金的锁定业务价格倒挂情况")
            ]
            
            # 查找每个表格的开始位置
            start_rows = []
            for pattern, name in table_patterns:
                found = False
                for idx, row in sheet_data.iterrows():
                    for j in range(len(row)):
                        cell_value = str(row[j]) if not pd.isna(row[j]) else ""
                        if re.search(pattern, cell_value):
                            print(f"找到保证金表格标题: {cell_value} 在行 {idx}")
                            # 直接使用Python的整数索引
                            try:
                                # 确保idx是整数
                                idx_int = int(idx) if not isinstance(idx, int) else idx
                                start_row = idx_int + 1
                                start_rows.append((start_row, name))
                                found = True
                                break
                            except (ValueError, TypeError):
                                print(f"警告: 行索引转换为整数失败: {idx}，类型: {type(idx)}")
                                continue
                    if found:
                        break
                if not found:
                    print(f"警告: 未找到保证金表格 '{name}'")
            
            # 按行号排序表格起始位置
            start_rows.sort(key=lambda x: x[0])
            
            # 计算每个表格的结束位置
            for i in range(len(start_rows)):
                start_row, table_name = start_rows[i]
                
                # 如果不是最后一个表格，则下一个表格的开始行是当前表格的结束行
                if i < len(start_rows) - 1:
                    end_row = start_rows[i + 1][0] - 1
                else:
                    # 对于最后一个表格，查找连续的空行作为结束标志
                    end_row = self._find_table_end(sheet_data, start_row)
                
                # 提取表格数据，确保至少包含两行（标题行和至少一行数据）
                if end_row - start_row < 1:
                    print(f"警告: 保证金表格 '{table_name}' 太短，可能无效")
                    continue
                
                table_data = sheet_data.iloc[start_row:end_row+1].copy()
                
                # 处理标题行，设置为DataFrame的列名
                headers = table_data.iloc[0].fillna('')
                table_data = table_data.iloc[1:].copy()
                table_data.columns = headers
                
                # 清除空行
                table_data = table_data.dropna(how='all')
                
                # 存储表格数据
                self.deposit_tables[table_name] = table_data
                print(f"成功读取保证金表格: {table_name}, 行数: {len(table_data)}")
                
            return len(self.deposit_tables) > 0
            
        except Exception as e:
            print(f"读取保证金sheet时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def _find_table_end(self, df: pd.DataFrame, start_row: int, empty_threshold: int = 3) -> int:
        """查找表格结束位置，通过检测连续空行"""
        empty_count = 0
        for i in range(start_row + 1, len(df)):
            # 检查行是否为空（所有单元格都是NaN或空字符串）
            row_values = df.iloc[i].fillna('').astype(str)
            if row_values.str.strip().str.len().sum() == 0 or row_values.isna().all():
                empty_count += 1
                if empty_count >= empty_threshold:
                    return i - empty_threshold
            else:
                empty_count = 0
                
        return len(df) - 1
    
    def get_tables(self) -> Dict[str, pd.DataFrame]:
        """获取所有读取的表格"""
        return self.tables
    
    def get_deposit_tables(self) -> Dict[str, pd.DataFrame]:
        """获取保证金相关表格"""
        return self.deposit_tables
    
    def get_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """获取指定名称的表格"""
        return self.tables.get(table_name, None)
    
    def get_deposit_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """获取指定名称的保证金表格"""
        return self.deposit_tables.get(table_name, None)
    
    def get_processed_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """获取指定名称的处理后表格"""
        return self.processed_tables.get(table_name, None)
    
    def process_overdue_payment(self, threshold: float = 3000) -> bool:
        """处理'一、逾期还款业务'表"""
        table_name = "一、逾期还款业务"
        table = self.get_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 查找合同号列，有可能名称不完全一致
            contract_column = None
            possible_contract_columns = ['合同号', '合同', '合同编号', '合同序号', '编号']
            for col in possible_contract_columns:
                if col in df.columns:
                    contract_column = col
                    print(f"找到合同号列: {col}")
                    break
            
            if contract_column is None:
                print("警告: 未找到合同号列，将使用所有行")
                # 1. 清除所有合计行 - 由于没有合同号列，无法使用合同号过滤
                # 尝试查找合计行或小计行，通常这些行的第一列或者经营单位列会包含"合计"或"小计"字样
                if '经营单位' in df.columns:
                    df = df[~df['经营单位'].astype(str).str.contains('合计|小计', na=False)]
                    print("已通过'经营单位'列过滤合计行")
                elif df.columns[0] in df.columns:  # 使用第一列
                    first_col = df.columns[0]
                    df = df[~df[first_col].astype(str).str.contains('合计|小计', na=False)]
                    print(f"已通过第一列'{first_col}'过滤合计行")
            else:
                # 1. 清除所有合计行（通过清除所有合同号为空的行来实现）
                df = df[df[contract_column].notna()]
                print(f"已通过'{contract_column}'列过滤合计行，剩余行数: {len(df)}")
            
            # 将二级部门为"启宏实业"的数据中的客户列值都改为"启宏实业"
            if '二级部门' in df.columns and '客户' in df.columns:
                # 找出二级部门为"启宏实业"的行
                qihong_mask = df['二级部门'].astype(str).str.contains('启宏实业', na=False)
                if qihong_mask.any():
                    # 修改这些行的客户列值
                    df.loc[qihong_mask, '客户'] = '启宏实业'
                    print(f"已将{qihong_mask.sum()}行二级部门为'启宏实业'的记录的客户改为'启宏实业'")
            
            # 检查是否有必要的列
            required_columns = ['逾期事由', '金额/万元', '板群', '经营单位', '客户', '产品']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 缺少必要的列: {missing_columns}")
                # 尝试查找替代列
                column_maps = {
                    '逾期事由': ['逾期原因', '事由', '原因'],
                    '金额/万元': ['金额', '金额(万元)', '金额（万元）', '逾期金额', '逾期金额/万元'],
                    '板群': ['分板群', '业务板群'],
                    '经营单位': ['经营单位名称', '单位', '部门'],
                    '客户': ['客户名称', '客户名', '购货单位'],
                    '产品': ['产品名称', '品名', '货物']
                }
                
                # 创建列名映射
                column_mapping = {}
                for required_col, alternatives in column_maps.items():
                    if required_col not in df.columns:
                        for alt_col in alternatives:
                            if alt_col in df.columns:
                                column_mapping[alt_col] = required_col
                                print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                                break
                
                # 重命名列
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    print(f"列重命名后的列名: {list(df.columns)}")
            
            # 检查重命名后是否还有缺失的列
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 仍然缺少必要的列: {missing_columns}")
                return False
            
            # 2. 增加两列：【控货类业务逾期金额】和【授信类业务逾期金额】
            df['控货类业务逾期金额'] = 0.0
            df['授信类业务逾期金额'] = 0.0
            
            # 应用规则
            control_cargo_conditions = (df['逾期事由'] == '控货逾期未收款') | (df['逾期事由'] == '已出运未收汇（非OA）')
            
            # 设置控货类业务逾期金额
            df.loc[control_cargo_conditions, '控货类业务逾期金额'] = df.loc[control_cargo_conditions, '金额/万元']
            
            # 设置授信类业务逾期金额
            df.loc[~control_cargo_conditions, '授信类业务逾期金额'] = df.loc[~control_cargo_conditions, '金额/万元']
            
            # 3. 检查并保留指定列
            columns_to_keep = ['板群', '经营单位', '客户', '产品', '金额/万元', '控货类业务逾期金额', '授信类业务逾期金额', 
                              '本周还款计划', '集团在手(万元)', '集团占用(万元)']
            
            # 检查并处理可能缺少的非必要列
            for col in ['本周还款计划', '集团在手(万元)', '集团占用(万元)']:
                if col not in df.columns:
                    print(f"警告: 列 '{col}' 不存在，将添加空列")
                    df[col] = ''
            
            # 保留指定列
            df_simplified = df[columns_to_keep].copy()
            
            # 4. 按照板群、经营单位、客户作为维度合并数据
            def combine_text_values(series) -> str:
                """合并文本值，去重并用'/'分隔"""
                unique_values = list(set(str(val) for val in series if not pd.isna(val) and str(val).strip() != ''))
                return '/'.join(unique_values) if unique_values else ''
            
            def sum_numeric_values(series) -> float:
                """合计数值"""
                return series.sum()
            
            # 定义每列的聚合函数
            aggregations = {
                '产品': combine_text_values,
                '金额/万元': sum_numeric_values,
                '控货类业务逾期金额': sum_numeric_values,
                '授信类业务逾期金额': sum_numeric_values,
                '本周还款计划': combine_text_values,
                '集团在手(万元)': combine_text_values,
                '集团占用(万元)': combine_text_values
            }
            
            # 执行分组聚合
            grouped_df = df_simplified.groupby(['板群', '经营单位', '客户']).agg(aggregations).reset_index()
            
            # 5. 按照金额筛选，只有【金额/万元】合计超过阈值的客户才需要列出
            # 确保金额列为数值类型
            grouped_df['金额/万元'] = pd.to_numeric(grouped_df['金额/万元'], errors='coerce')
            
            # 计算每个客户的金额合计
            customer_amounts = grouped_df.groupby('客户')['金额/万元'].sum()
            large_customers = customer_amounts[customer_amounts >= threshold].index.tolist()
            
            print(f"金额合计超过{threshold}万的客户数量: {len(large_customers)}")
            
            # 分离大客户和小客户
            large_df = grouped_df[grouped_df['客户'].isin(large_customers)]
            small_df = grouped_df[~grouped_df['客户'].isin(large_customers)]
            
            # 按照经营单位合并小客户
            if not small_df.empty:
                # 为小客户创建一个"其他"客户
                small_df['客户'] = '其他'
                
                # 将指定列的内容改为"见明细表"
                columns_to_modify = ['产品', '本周还款计划', '集团在手(万元)', '集团占用(万元)']
                for col in columns_to_modify:
                    if col in small_df.columns:
                        small_df[col] = '见明细表'
                
                # 按照板群和经营单位重新聚合
                small_df_agg = small_df.groupby(['板群', '经营单位', '客户']).agg(aggregations).reset_index()
                
                # 合并大客户和聚合后的小客户
                final_df = pd.concat([large_df, small_df_agg], axis=0).reset_index(drop=True)
            else:
                final_df = large_df.copy()
            
            # 存储处理后的表格
            self.processed_tables[table_name] = final_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(final_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def process_overdue_delivery(self, threshold: float = 3000) -> bool:
        """处理'二、付款逾期未到货(1)'表"""
        table_name = "二、付款逾期未到货(1)"
        table = self.get_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 查找合同号列，有可能名称不完全一致
            contract_column = None
            possible_contract_columns = ['合同号', '合同', '合同编号', '合同序号', '编号']
            for col in possible_contract_columns:
                if col in df.columns:
                    contract_column = col
                    print(f"找到合同号列: {col}")
                    break
            
            if contract_column is None:
                print("警告: 未找到合同号列，将使用所有行")
                # 1. 清除所有合计行 - 由于没有合同号列，无法使用合同号过滤
                # 尝试查找合计行或小计行，通常这些行的第一列或者经营单位列会包含"合计"或"小计"字样
                if '经营单位' in df.columns:
                    df = df[~df['经营单位'].astype(str).str.contains('合计|小计', na=False)]
                    print("已通过'经营单位'列过滤合计行")
                elif df.columns[0] in df.columns:  # 使用第一列
                    first_col = df.columns[0]
                    df = df[~df[first_col].astype(str).str.contains('合计|小计', na=False)]
                    print(f"已通过第一列'{first_col}'过滤合计行")
            else:
                # 1. 清除所有合计行（通过清除所有合同号为空的行来实现）
                df = df[df[contract_column].notna()]
                print(f"已通过'{contract_column}'列过滤合计行，剩余行数: {len(df)}")
            
            # 检查是否有必要的列
            required_columns = ['逾期事由', '金额/万元', '板群', '经营单位', '供应商', '产品']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 缺少必要的列: {missing_columns}")
                # 尝试查找替代列
                column_maps = {
                    '逾期事由': ['逾期原因', '事由', '原因'],
                    '金额/万元': ['金额', '金额(万元)', '金额（万元）', '逾期金额', '逾期金额/万元'],
                    '板群': ['分板群', '业务板群'],
                    '经营单位': ['经营单位名称', '单位', '部门'],
                    '供应商': ['供应商名称', '供应商名', '供货单位'],
                    '产品': ['产品名称', '品名', '货物']
                }
                
                # 创建列名映射
                column_mapping = {}
                for required_col, alternatives in column_maps.items():
                    if required_col not in df.columns:
                        for alt_col in alternatives:
                            if alt_col in df.columns:
                                column_mapping[alt_col] = required_col
                                print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                                break
                
                # 重命名列
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    print(f"列重命名后的列名: {list(df.columns)}")
            
            # 检查重命名后是否还有缺失的列
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 仍然缺少必要的列: {missing_columns}")
                return False
            
            # 2. 增加两列：【控货类业务逾期金额】和【授信类业务逾期金额】
            df['控货类业务逾期金额'] = 0.0
            df['授信类业务逾期金额'] = 0.0
            
            # 应用规则
            control_cargo_conditions = (df['逾期事由'] == '控货逾期未收款') | (df['逾期事由'] == '已出运未收汇（非OA）')
            
            # 设置控货类业务逾期金额
            df.loc[control_cargo_conditions, '控货类业务逾期金额'] = df.loc[control_cargo_conditions, '金额/万元']
            
            # 设置授信类业务逾期金额
            df.loc[~control_cargo_conditions, '授信类业务逾期金额'] = df.loc[~control_cargo_conditions, '金额/万元']
            
            # 3. 检查并保留指定列
            columns_to_keep = ['板群', '经营单位', '供应商', '产品', '金额/万元', '控货类业务逾期金额', '授信类业务逾期金额', 
                              '本周到货计划', '集团在手(万元)', '集团占用(万元)']
            
            # 检查并处理可能缺少的非必要列
            for col in ['本周到货计划', '集团在手(万元)', '集团占用(万元)']:
                if col not in df.columns:
                    print(f"警告: 列 '{col}' 不存在，将添加空列")
                    df[col] = ''
            
            # 保留指定列
            df_simplified = df[columns_to_keep].copy()
            
            # 4. 按照板群、经营单位、供应商作为维度合并数据
            def combine_text_values(series) -> str:
                """合并文本值，去重并用'/'分隔"""
                unique_values = list(set(str(val) for val in series if not pd.isna(val) and str(val).strip() != ''))
                return '/'.join(unique_values) if unique_values else ''
            
            def sum_numeric_values(series) -> float:
                """合计数值"""
                return series.sum()
            
            # 定义每列的聚合函数
            aggregations = {
                '产品': combine_text_values,
                '金额/万元': sum_numeric_values,
                '控货类业务逾期金额': sum_numeric_values,
                '授信类业务逾期金额': sum_numeric_values,
                '本周到货计划': combine_text_values,
                '集团在手(万元)': combine_text_values,
                '集团占用(万元)': combine_text_values
            }
            
            # 执行分组聚合
            grouped_df = df_simplified.groupby(['板群', '经营单位', '供应商']).agg(aggregations).reset_index()
            
            # 5. 按照金额筛选，只有【金额/万元】合计超过阈值的供应商才需要列出
            # 确保金额列为数值类型
            grouped_df['金额/万元'] = pd.to_numeric(grouped_df['金额/万元'], errors='coerce')
            
            # 计算每个供应商的金额合计
            supplier_amounts = grouped_df.groupby('供应商')['金额/万元'].sum()
            large_suppliers = supplier_amounts[supplier_amounts >= threshold].index.tolist()
            
            print(f"金额合计超过{threshold}万的供应商数量: {len(large_suppliers)}")
            
            # 分离大供应商和小供应商
            large_df = grouped_df[grouped_df['供应商'].isin(large_suppliers)]
            small_df = grouped_df[~grouped_df['供应商'].isin(large_suppliers)]
            
            # 按照经营单位合并小供应商
            if not small_df.empty:
                # 为小供应商创建一个"其他"供应商
                small_df['供应商'] = '其他'
                
                # 将指定列的内容改为"见明细表"
                columns_to_modify = ['产品', '本周到货计划', '集团在手(万元)', '集团占用(万元)']
                for col in columns_to_modify:
                    if col in small_df.columns:
                        small_df[col] = '见明细表'
                
                # 按照板群和经营单位重新聚合
                small_df_agg = small_df.groupby(['板群', '经营单位', '供应商']).agg(aggregations).reset_index()
                
                # 合并大供应商和聚合后的小供应商
                final_df = pd.concat([large_df, small_df_agg], axis=0).reset_index(drop=True)
            else:
                final_df = large_df.copy()
            
            # 存储处理后的表格
            self.processed_tables[table_name] = final_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(final_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def process_inventory(self, threshold: float = 3000) -> bool:
        """处理'八、现货敞口90天及以上库存'表"""
        table_name = "八、现货敞口90天及以上库存"
        table = self.get_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 1. 清除所有合计行（通过清除所有【产品】为空的行来实现）
            if '产品' in df.columns:
                df = df[df['产品'].notna()]
                print(f"已通过'产品'列过滤合计行，剩余行数: {len(df)}")
            else:
                print("警告: 未找到'产品'列，无法过滤合计行")
                
                # 尝试查找替代列
                possible_product_columns = ['品名', '货物', '产品名称', '商品']
                product_column = None
                for col in possible_product_columns:
                    if col in df.columns:
                        product_column = col
                        print(f"使用替代列 '{col}' 作为产品列")
                        df = df[df[product_column].notna()]
                        print(f"已通过'{product_column}'列过滤合计行，剩余行数: {len(df)}")
                        # 重命名列
                        df = df.rename(columns={product_column: '产品'})
                        break
            
            # 检查是否有必要的列
            required_columns = ['板群', '经营单位', '库存地点', '产品', '库存/万元', '本周处理计划']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 缺少必要的列: {missing_columns}")
                # 尝试查找替代列
                column_maps = {
                    '板群': ['分板群', '业务板群'],
                    '经营单位': ['经营单位名称', '单位', '部门'],
                    '库存地点': ['存放地点', '仓库', '仓储地点', '存储地点'],
                    '产品': ['品名', '货物', '产品名称', '商品'],
                    '库存/万元': ['金额/万元', '金额', '库存金额', '库存金额/万元', '库存额', '库存额/万元'],
                    '本周处理计划': ['处理计划', '本周计划', '计划']
                }
                
                # 创建列名映射
                column_mapping = {}
                for required_col, alternatives in column_maps.items():
                    if required_col not in df.columns:
                        for alt_col in alternatives:
                            if alt_col in df.columns:
                                column_mapping[alt_col] = required_col
                                print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                                break
                
                # 重命名列
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    print(f"列重命名后的列名: {list(df.columns)}")
            
            # 检查重命名后是否还有缺失的列
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 仍然缺少必要的列: {missing_columns}")
                return False
            
            # 2. 保留指定列
            columns_to_keep = ['板群', '经营单位', '库存地点', '产品', '库存/万元', '本周处理计划']
            
            # 检查并处理可能缺少的非必要列
            for col in ['本周处理计划']:
                if col not in df.columns:
                    print(f"警告: 列 '{col}' 不存在，将添加空列")
                    df[col] = ''
            
            # 保留指定列
            df_simplified = df[columns_to_keep].copy()
            
            # 3. 按照板群、经营单位、库存地点作为维度合并数据
            def combine_text_values(series) -> str:
                """合并文本值，去重并用'/'分隔"""
                unique_values = list(set(str(val) for val in series if not pd.isna(val) and str(val).strip() != ''))
                return '/'.join(unique_values) if unique_values else ''
            
            def sum_numeric_values(series) -> float:
                """合计数值"""
                return series.sum()
            
            # 定义每列的聚合函数
            aggregations = {
                '产品': combine_text_values,
                '库存/万元': sum_numeric_values,
                '本周处理计划': combine_text_values
            }
            
            # 执行分组聚合
            grouped_df = df_simplified.groupby(['板群', '经营单位', '库存地点']).agg(aggregations).reset_index()
            
            # 确保金额列为数值类型
            grouped_df['库存/万元'] = pd.to_numeric(grouped_df['库存/万元'], errors='coerce')
            
            # 5. 按照金额筛选，只有【库存/万元】合计超过阈值的库存地点才需要列出
            # 计算每个库存地点的金额合计
            location_amounts = grouped_df.groupby('库存地点')['库存/万元'].sum()
            large_locations = location_amounts[location_amounts >= threshold].index.tolist()
            
            print(f"库存金额合计超过{threshold}万的库存地点数量: {len(large_locations)}")
            
            # 分离大金额和小金额库存地点
            large_df = grouped_df[grouped_df['库存地点'].isin(large_locations)]
            small_df = grouped_df[~grouped_df['库存地点'].isin(large_locations)]
            
            # 按照经营单位合并小金额库存地点
            if not small_df.empty:
                # 为小金额库存地点创建一个"其他"
                small_df['库存地点'] = '其他'
                
                # 将指定列的内容改为"见明细表"
                columns_to_modify = ['产品', '本周处理计划']
                for col in columns_to_modify:
                    if col in small_df.columns:
                        small_df[col] = '见明细表'
                
                # 按照板群和经营单位重新聚合
                small_df_agg = small_df.groupby(['板群', '经营单位', '库存地点']).agg(aggregations).reset_index()
                
                # 合并大金额和聚合后的小金额库存地点
                final_df = pd.concat([large_df, small_df_agg], axis=0).reset_index(drop=True)
            else:
                final_df = large_df.copy()
            
            # 存储处理后的表格
            self.processed_tables[table_name] = final_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(final_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def process_deposit_ratio(self, threshold: float = 300) -> bool:
        """处理'保证金比例低于合同约定比例'表"""
        table_name = "保证金比例低于合同约定比例"
        table = self.get_deposit_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 1. 清除所有合计行（通过清除所有【客户名称】为空的行来实现）
            if '客户名称' in df.columns:
                df = df[df['客户名称'].notna()]
                print(f"已通过'客户名称'列过滤合计行，剩余行数: {len(df)}")
            else:
                print("警告: 未找到'客户名称'列，尝试查找替代列")
                # 尝试查找替代列
                customer_cols = ['客户', '客户名', '企业名称', '购货单位']
                for col in customer_cols:
                    if col in df.columns:
                        df = df[df[col].notna()]
                        print(f"已通过'{col}'列过滤合计行，剩余行数: {len(df)}")
                        # 重命名列为标准名称
                        df = df.rename(columns={col: '客户名称'})
                        break
            
            # 检查是否有必要的列
            required_columns = ['经营单位', '客户名称', '商品', '实际保证金比例', 
                              '补至0% 需追加 （万元）', '补至10%需追加 （万元，合同约定低于10%则按合同约定）', 
                              '在手业务金额（万元）', '备注']
            
            # 尝试处理列名称，有些列名可能略有不同
            column_maps = {
                '经营单位': ['经营单位名称', '单位', '部门'],
                '客户名称': ['客户', '客户名', '企业名称', '购货单位'],
                '商品': ['产品', '品名', '货物', '产品名称'],
                '实际保证金比例': ['保证金比例', '实际比例', '保证金'],
                '补至0% 需追加 （万元）': ['补至0%需追加', '补至0%', '0%追加额', '追加金额'],
                '补至10%需追加 （万元，合同约定低于10%则按合同约定）': ['补至10%需追加', '补至10%', '10%追加额'],
                '在手业务金额（万元）': ['在手金额', '业务金额', '在手业务', '合同金额', '在手合同金额'],
                '备注': ['说明', '注释', '附注']
            }
            
            # 创建列名映射
            column_mapping = {}
            missing_columns = []
            
            for required_col in required_columns:
                if required_col not in df.columns:
                    alternatives = column_maps.get(required_col, [])
                    found = False
                    for alt_col in alternatives:
                        if alt_col in df.columns:
                            column_mapping[alt_col] = required_col
                            print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                            found = True
                            break
                    if not found:
                        missing_columns.append(required_col)
                        print(f"警告: 未找到列 '{required_col}' 或其替代列")
            
            # 重命名列
            if column_mapping:
                df = df.rename(columns=column_mapping)
                print(f"列重命名后的列名: {list(df.columns)}")
            
            # 如果有缺失的必要列，仍然尝试继续处理
            for col in missing_columns:
                if col not in df.columns:
                    print(f"警告: 缺少必要的列 '{col}'，将添加空列")
                    df[col] = ''
            
            # 2. 保留指定列
            columns_to_keep = required_columns
            try:
                df_simplified = df[columns_to_keep].copy()
                print(f"保留指定列后的列名: {list(df_simplified.columns)}")
            except KeyError as e:
                print(f"错误: 某些指定列在数据框中不存在: {str(e)}")
                # 使用可用的列
                available_columns = [col for col in columns_to_keep if col in df.columns]
                df_simplified = df[available_columns].copy()
                print(f"使用可用列后的列名: {list(df_simplified.columns)}")
            
            # 3. 筛选【补至0% 需追加 （万元）】大于阈值(默认300)的数据
            filter_col = '补至0% 需追加 （万元）'
            if filter_col in df_simplified.columns:
                # 确保该列为数值类型
                df_simplified[filter_col] = pd.to_numeric(df_simplified[filter_col], errors='coerce')
                
                # 筛选大于阈值的数据
                filtered_df = df_simplified[df_simplified[filter_col] > threshold]
                print(f"筛选{filter_col} > {threshold}后的行数: {len(filtered_df)}")
            else:
                print(f"警告: 未找到筛选列 '{filter_col}'，跳过筛选步骤")
                filtered_df = df_simplified
            
            # 存储处理后的表格
            self.processed_tables[table_name] = filtered_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(filtered_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def process_deposit_inversion(self, threshold: float = 3000) -> bool:
        """处理'未约定收保证金的锁定业务价格倒挂情况'表"""
        table_name = "未约定收保证金的锁定业务价格倒挂情况"
        table = self.get_deposit_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 1. 清除所有合计行（通过清除所有【客户名称】为空的行来实现）
            if '客户名称' in df.columns:
                df = df[df['客户名称'].notna()]
                print(f"已通过'客户名称'列过滤合计行，剩余行数: {len(df)}")
            else:
                print("警告: 未找到'客户名称'列，尝试查找替代列")
                # 尝试查找替代列
                customer_cols = ['客户', '客户名', '企业名称', '购货单位']
                for col in customer_cols:
                    if col in df.columns:
                        df = df[df[col].notna()]
                        print(f"已通过'{col}'列过滤合计行，剩余行数: {len(df)}")
                        # 重命名列为标准名称
                        df = df.rename(columns={col: '客户名称'})
                        break
            
            # 检查是否有必要的列
            required_columns = ['经营单位', '客户名称', '商品', '实际保证金比例', 
                              '补至0% 需追加 （万元）', '补至10%需追加 （万元，合同约定低于10%则按合同约定）', 
                              '在手业务金额（万元）', '备注']
            
            # 尝试处理列名称，有些列名可能略有不同
            column_maps = {
                '经营单位': ['经营单位名称', '单位', '部门'],
                '客户名称': ['客户', '客户名', '企业名称', '购货单位'],
                '商品': ['产品', '品名', '货物', '产品名称'],
                '实际保证金比例': ['保证金比例', '实际比例', '保证金'],
                '补至0% 需追加 （万元）': ['补至0%需追加', '补至0%', '0%追加额', '追加金额'],
                '补至10%需追加 （万元，合同约定低于10%则按合同约定）': ['补至10%需追加', '补至10%', '10%追加额'],
                '在手业务金额（万元）': ['在手金额', '业务金额', '在手业务', '合同金额', '在手合同金额'],
                '备注': ['说明', '注释', '附注']
            }
            
            # 创建列名映射
            column_mapping = {}
            missing_columns = []
            
            for required_col in required_columns:
                if required_col not in df.columns:
                    alternatives = column_maps.get(required_col, [])
                    found = False
                    for alt_col in alternatives:
                        if alt_col in df.columns:
                            column_mapping[alt_col] = required_col
                            print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                            found = True
                            break
                    if not found:
                        missing_columns.append(required_col)
                        print(f"警告: 未找到列 '{required_col}' 或其替代列")
            
            # 重命名列
            if column_mapping:
                df = df.rename(columns=column_mapping)
                print(f"列重命名后的列名: {list(df.columns)}")
            
            # 如果有缺失的必要列，仍然尝试继续处理
            for col in missing_columns:
                if col not in df.columns:
                    print(f"警告: 缺少必要的列 '{col}'，将添加空列")
                    df[col] = ''
            
            # 2. 保留指定列
            columns_to_keep = required_columns
            try:
                df_simplified = df[columns_to_keep].copy()
                print(f"保留指定列后的列名: {list(df_simplified.columns)}")
            except KeyError as e:
                print(f"错误: 某些指定列在数据框中不存在: {str(e)}")
                # 使用可用的列
                available_columns = [col for col in columns_to_keep if col in df.columns]
                df_simplified = df[available_columns].copy()
                print(f"使用可用列后的列名: {list(df_simplified.columns)}")
            
            # 3. 筛选【补至0% 需追加 （万元）】大于阈值(默认3000)的数据
            filter_col = '补至0% 需追加 （万元）'
            if filter_col in df_simplified.columns:
                # 确保该列为数值类型
                df_simplified[filter_col] = pd.to_numeric(df_simplified[filter_col], errors='coerce')
                
                # 筛选大于阈值的数据
                filtered_df = df_simplified[df_simplified[filter_col] > threshold]
                print(f"筛选{filter_col} > {threshold}后的行数: {len(filtered_df)}")
            else:
                print(f"警告: 未找到筛选列 '{filter_col}'，跳过筛选步骤")
                filtered_df = df_simplified
            
            # 存储处理后的表格
            self.processed_tables[table_name] = filtered_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(filtered_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def read_future_sheet(self) -> bool:
        """读取Excel文件中的'未定价或远期交货业务'sheet页"""
        if not self.file_path:
            return False
            
        try:
            # 读取未定价或远期交货业务工作表
            print(f"正在读取未定价或远期交货业务sheet: {self.file_path}")
            sheet_data = pd.read_excel(self.file_path, sheet_name="未定价或远期交货业务", header=None)
            
            # 只读取【汇总表（事业部及以上领导见本汇总表即可）】表格
            table_name = "汇总表（事业部及以上领导见本汇总表即可）"
            
            # 查找表格的开始位置（表格标题行）
            title_row = None
            for idx, row in sheet_data.iterrows():
                for j in range(len(row)):
                    cell_value = str(row[j]) if not pd.isna(row[j]) else ""
                    # 兼容全角和半角括号
                    if "汇总表" in cell_value and ("事业部" in cell_value or "领导" in cell_value):
                        print(f"找到未定价或远期交货业务表格标题: {cell_value} 在行 {idx}")
                        title_row = idx
                        break
                if title_row is not None:
                    break
                    
            if title_row is None:
                print(f"警告: 未找到表格 '{table_name}'")
                return False
            
            # 查找表格头部（列名所在行）
            # 通常在表格标题的下一行或隔一行
            header_row = None
            if title_row is not None:
                title_row_idx = int(title_row)  # 确保title_row为整数
                for i in range(title_row_idx + 1, title_row_idx + 5):  # 检查标题后的几行
                    if i >= len(sheet_data):
                        break
                    
                    row = sheet_data.iloc[i]
                    # 检查是否包含常见的列名如"部门"、"供应商/客户"等
                    for col_idx, cell in enumerate(row):
                        cell_str = str(cell).strip() if not pd.isna(cell) else ""
                        if cell_str in ["部门", "供应商/客户", "类型", "履约风险值（元）"]:
                            header_row = i
                            print(f"找到表格头部在行 {header_row}，列名: {cell_str}")
                            break
                    
                    if header_row is not None:
                        break

            if header_row is None and title_row is not None:
                # 如果没有找到明确的表头行，默认使用标题行的下一行
                title_row_idx = int(title_row)  # 确保title_row为整数
                header_row = title_row_idx + 1
                print(f"未找到明确的表头行，使用默认表头行: {header_row}")

            # 表格数据从头部行的下一行开始
            if header_row is not None:
                start_row = header_row + 1
                print(f"表格数据开始于行: {start_row}")
            
            # 查找'履约风险值（元）'列的索引
            headers = sheet_data.iloc[header_row].fillna('')
            
            risk_value_col_idx = None
            risk_value_col_names = ['履约风险值（元）', '风险值', '风险值（元）', '风险金额', '风险金额（元）']
            
            for col_idx, col_name in enumerate(headers):
                col_name_str = str(col_name).strip()
                if any(risk_name in col_name_str for risk_name in risk_value_col_names):
                    risk_value_col_idx = col_idx
                    print(f"找到履约风险值列: '{col_name_str}' 在列索引 {col_idx}")
                    break
            
            # 查找表格的结束位置
            end_row = None
            
            if risk_value_col_idx is None:
                print("警告: 未找到履约风险值列，使用默认的结束位置查找逻辑")
                for idx in range(start_row, len(sheet_data)):
                    row_values = sheet_data.iloc[idx].fillna('').astype(str)
                    if "采购远期交货业务" in ' '.join(row_values) or "销售远期交货业务" in ' '.join(row_values):
                        end_row = idx - 1
                        print(f"找到新表格开始标记，汇总表结束行: {end_row}")
                        break
            else:
                # 查找第一个履约风险值为空但其他列不为空的行
                for idx in range(start_row, len(sheet_data)):
                    # # 首先检查是否有新表格开始
                    # row_values = sheet_data.iloc[idx].fillna('').astype(str)
                    # if "采购远期交货业务" in ' '.join(row_values) or "销售远期交货业务" in ' '.join(row_values):
                    #     end_row = idx - 1
                    #     print(f"找到新表格开始标记，汇总表结束行: {end_row}")
                    #     break
                    
                    # 检查履约风险值是否为空
                    risk_value = sheet_data.iloc[idx, risk_value_col_idx]
                    if pd.isna(risk_value) or str(risk_value).strip() == '':
                        # 确认这一行的其他列是否有数据（不是完全空行）
                        other_cols_have_data = False
                        for j in range(len(sheet_data.iloc[idx])):
                            if j != risk_value_col_idx:
                                cell_value = sheet_data.iloc[idx, j]
                                if not pd.isna(cell_value) and str(cell_value).strip() != '':
                                    other_cols_have_data = True
                                    break
                        
                        if other_cols_have_data:
                            # 第一个履约风险值为空但其他列不为空的行表示表格结束
                            end_row = idx - 1  # 不包括这一行
                            print(f"找到履约风险值为空的行，汇总表结束行: {end_row}")
                            break
            
            # 如果没有找到结束行，使用工作表的末尾
            if end_row is None or end_row < start_row:
                # 继续向下查找，直到出现空行或新表格开始标记
                for idx in range(start_row, len(sheet_data)):
                    row_is_empty = True
                    for j in range(len(sheet_data.iloc[idx])):
                        cell_value = sheet_data.iloc[idx, j]
                        if not pd.isna(cell_value) and str(cell_value).strip() != '':
                            row_is_empty = False
                            break
                    
                    if row_is_empty:
                        end_row = idx - 1
                        print(f"找到空行，汇总表结束行: {end_row}")
                        break
                
                if end_row is None or end_row < start_row:
                    end_row = len(sheet_data) - 1
                    print(f"未找到明确的结束标记，使用默认结束行: {end_row}")
            
            # 提取表格数据，包括表头行
            table_data = sheet_data.iloc[header_row:end_row+1].copy()
            
            # 获取列名并处理数据
            headers = table_data.iloc[0].fillna('')
            table_data = table_data.iloc[1:].copy()
            table_data.columns = headers
            
            # 清除空行
            table_data = table_data.dropna(how='all')
            
            # 输出列名以便调试
            print(f"表格列名: {list(table_data.columns)}")
            
            # 存储表格数据
            self.future_tables[table_name] = table_data
            print(f"成功读取未定价或远期交货业务表格: {table_name}, 行数: {len(table_data)}")
            
            return True
            
        except Exception as e:
            print(f"读取未定价或远期交货业务sheet时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def process_future_summary(self, threshold: float = -30000000) -> bool:
        """处理'汇总表（事业部及以上领导见本汇总表即可）'表"""
        table_name = "汇总表（事业部及以上领导见本汇总表即可）"
        table = self.get_future_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 1. 清除所有合计行（通过清除所有【部门】为空的行来实现）
            dept_cols = ['部门', '经营单位', '单位']
            dept_col = None
            for col in dept_cols:
                if col in df.columns:
                    dept_col = col
                    df = df[df[col].notna()]
                    print(f"已通过'{col}'列过滤合计行，剩余行数: {len(df)}")
                    break
                    
            if dept_col is None:
                print("警告: 未找到部门相关列，尝试使用第一列过滤")
                first_col = df.columns[0]
                df = df[df[first_col].notna()]
                print(f"已通过第一列'{first_col}'过滤合计行，剩余行数: {len(df)}")
                
            # 2. 填充"类型"列的合并单元格值
            if '类型' in df.columns:
                # 使用可靠的方式填充合并单元格
                # 先转换成列表处理再转回DataFrame
                type_values = df['类型'].values
                last_valid_value = None
                for i in range(len(type_values)):
                    if pd.notna(type_values[i]) and str(type_values[i]).strip() != '':
                        last_valid_value = type_values[i]
                    elif last_valid_value is not None:
                        type_values[i] = last_valid_value
                df['类型'] = type_values
                print("已填充'类型'列的合并单元格值")
            else:
                print("警告: 未找到'类型'列")
                # 尝试查找替代列
                type_cols = ['业务类型', '品种类型', '种类']
                for col in type_cols:
                    if col in df.columns:
                        # 使用可靠的方式填充合并单元格
                        type_values = df[col].values
                        last_valid_value = None
                        for i in range(len(type_values)):
                            if pd.notna(type_values[i]) and str(type_values[i]).strip() != '':
                                last_valid_value = type_values[i]
                            elif last_valid_value is not None:
                                type_values[i] = last_valid_value
                        df[col] = type_values
                        df = df.rename(columns={col: '类型'})
                        print(f"使用替代列 '{col}' 并填充合并单元格值")
                        break
            
            # 3. 调整字段顺序，按照【部门】、【供应商/客户】、【类型】、【履约风险值（元）】、【备注】
            # 首先确认需要的列存在，如果不存在则尝试查找替代列
            column_maps = {
                '部门': ['经营单位', '单位', '部门名称'],
                '供应商/客户': ['供应商', '客户', '客户名称', '供应商名称'],
                '类型': ['业务类型', '品种类型', '种类'],
                '履约风险值（元）': ['风险值', '风险值（元）', '风险金额', '风险金额（元）'],
                '备注': ['说明', '注释', '附注']
            }
            
            # 创建列名映射
            column_mapping = {}
            for required_col, alternatives in column_maps.items():
                if required_col not in df.columns:
                    for alt_col in alternatives:
                        if alt_col in df.columns:
                            column_mapping[alt_col] = required_col
                            print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                            break
            
            # 重命名列
            if column_mapping:
                df = df.rename(columns=column_mapping)
                print(f"列重命名后的列名: {list(df.columns)}")
            
            # 确保需要的列存在
            for col in ['部门', '供应商/客户', '类型', '履约风险值（元）', '备注']:
                if col not in df.columns:
                    print(f"警告: 未找到列 '{col}'，将添加空列")
                    df[col] = ''
            
            # 按指定顺序保留列
            columns_to_keep = ['部门', '供应商/客户', '类型', '履约风险值（元）', '备注']
            df_simplified = df[columns_to_keep].copy()
            print(f"调整字段顺序后的列名: {list(df_simplified.columns)}")
            
            # 4. 筛选【履约风险值（元）】小于阈值(默认-30000000)的数据
            filter_col = '履约风险值（元）'
            # 确保该列为数值类型
            df_simplified[filter_col] = pd.to_numeric(df_simplified[filter_col], errors='coerce')
            
            # 筛选小于阈值的数据
            filtered_df = df_simplified[df_simplified[filter_col] < threshold]
            print(f"筛选{filter_col} < {threshold}后的行数: {len(filtered_df)}")
            
            # 存储处理后的表格
            self.processed_tables[table_name] = filtered_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(filtered_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
            
    def get_future_tables(self) -> Dict[str, pd.DataFrame]:
        """获取未定价或远期交货业务相关表格"""
        return self.future_tables
        
    def get_future_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """获取指定名称的未定价或远期交货业务表格"""
        return self.future_tables.get(table_name, None)


class ThresholdInputDialog(QDialog):
    """金额阈值输入对话框"""
    
    def __init__(self, parent=None, default_value: float = 3000.0):
        super().__init__(parent)
        self.setWindowTitle("输入金额阈值")
        self.setGeometry(300, 300, 300, 100)
        
        layout = QFormLayout()
        self.threshold_input = QLineEdit(str(default_value))
        layout.addRow("金额阈值(万元):", self.threshold_input)
        
        self.ok_button = QPushButton("确定")
        self.ok_button.clicked.connect(self.accept)
        
        self.cancel_button = QPushButton("取消")
        self.cancel_button.clicked.connect(self.reject)
        
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(self.cancel_button)
        
        main_layout = QVBoxLayout()
        main_layout.addLayout(layout)
        main_layout.addLayout(button_layout)
        
        self.setLayout(main_layout)
    
    def get_threshold(self):
        """获取输入的阈值"""
        try:
            return float(self.threshold_input.text())
        except ValueError:
            return 3000.0  # 默认值

class ExcelProcessorApp(QMainWindow):
    """Excel处理应用的GUI界面"""
    
    def __init__(self):
        super().__init__()
        
        self.excel_processor = ExcelProcessor()
        self.threshold = 3000.0  # 默认阈值
        self.init_ui()
        
    def init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle("Excel处理工具")
        self.setGeometry(100, 100, 1200, 800)
        
        # 创建最顶层的布局
        main_layout = QVBoxLayout()
        
        # 文件选择区域
        file_layout = QHBoxLayout()
        self.file_label = QLabel("未选择文件")
        self.file_button = QPushButton("选择Excel文件")
        self.file_button.clicked.connect(self.select_file)
        
        file_layout.addWidget(self.file_label)
        file_layout.addWidget(self.file_button)
        
        # 添加文件选择区域到主布局
        main_layout.addLayout(file_layout)
        
        # 创建最顶层的选项卡控件
        self.main_tab_widget = QTabWidget()
        
        # ====== 创建"贸易经营风险指标"选项卡内容 ======
        risk_tab = QWidget()
        risk_layout = QVBoxLayout()
        
        # 贸易经营风险指标处理按钮区域
        risk_button_layout = QHBoxLayout()
        self.process_button = QPushButton("读取表格")
        self.process_button.clicked.connect(self.process_excel)
        
        self.process_overdue_button = QPushButton("处理'逾期还款业务'表")
        self.process_overdue_button.clicked.connect(self.process_overdue_payment)
        self.process_overdue_button.setEnabled(False)
        
        self.process_delivery_button = QPushButton("处理'付款逾期未到货(1)'表")
        self.process_delivery_button.clicked.connect(self.process_overdue_delivery)
        self.process_delivery_button.setEnabled(False)
        
        self.process_inventory_button = QPushButton("处理'现货敞口90天及以上库存'表")
        self.process_inventory_button.clicked.connect(self.process_inventory)
        self.process_inventory_button.setEnabled(False)
        
        risk_button_layout.addWidget(self.process_button)
        risk_button_layout.addWidget(self.process_overdue_button)
        risk_button_layout.addWidget(self.process_delivery_button)
        risk_button_layout.addWidget(self.process_inventory_button)
        
        # 贸易经营风险指标状态标签
        self.risk_status_label = QLabel("准备就绪")
        
        # 贸易经营风险指标选项卡控件，用于显示读取的表格
        self.risk_tab_widget = QTabWidget()
        
        # 添加控件到贸易经营风险指标布局
        risk_layout.addLayout(risk_button_layout)
        risk_layout.addWidget(self.risk_status_label)
        risk_layout.addWidget(self.risk_tab_widget)
        
        risk_tab.setLayout(risk_layout)
        
        # ====== 创建"保证金"选项卡内容 ======
        deposit_tab = QWidget()
        deposit_layout = QVBoxLayout()
        
        # 保证金处理按钮区域
        deposit_button_layout = QHBoxLayout()
        
        self.process_deposit_button = QPushButton("读取'保证金'表")
        self.process_deposit_button.clicked.connect(self.process_deposit)
        
        self.process_deposit_ratio_button = QPushButton("处理'保证金比例低于合同约定比例'表")
        self.process_deposit_ratio_button.clicked.connect(self.process_deposit_ratio)
        self.process_deposit_ratio_button.setEnabled(False)
        
        self.process_deposit_inversion_button = QPushButton("处理'价格倒挂情况'表")
        self.process_deposit_inversion_button.clicked.connect(self.process_deposit_inversion)
        self.process_deposit_inversion_button.setEnabled(False)
        
        deposit_button_layout.addWidget(self.process_deposit_button)
        deposit_button_layout.addWidget(self.process_deposit_ratio_button)
        deposit_button_layout.addWidget(self.process_deposit_inversion_button)
        
        # 保证金状态标签
        self.deposit_status_label = QLabel("准备就绪")
        
        # 保证金选项卡控件，用于显示读取的表格
        self.deposit_tab_widget = QTabWidget()
        
        # 添加控件到保证金布局
        deposit_layout.addLayout(deposit_button_layout)
        deposit_layout.addWidget(self.deposit_status_label)
        deposit_layout.addWidget(self.deposit_tab_widget)
        
        deposit_tab.setLayout(deposit_layout)
        
        # ====== 创建"未定价或远期交货业务"选项卡内容 ======
        future_tab = QWidget()
        future_layout = QVBoxLayout()
        
        # 未定价或远期交货业务处理按钮区域
        future_button_layout = QHBoxLayout()
        
        self.process_future_button = QPushButton("读取'未定价或远期交货业务'表")
        self.process_future_button.clicked.connect(self.process_future)
        
        self.process_future_summary_button = QPushButton("处理'汇总表'")
        self.process_future_summary_button.clicked.connect(self.process_future_summary)
        self.process_future_summary_button.setEnabled(False)
        
        future_button_layout.addWidget(self.process_future_button)
        future_button_layout.addWidget(self.process_future_summary_button)
        
        # 未定价或远期交货业务状态标签
        self.future_status_label = QLabel("准备就绪")
        
        # 未定价或远期交货业务选项卡控件，用于显示读取的表格
        self.future_tab_widget = QTabWidget()
        
        # 添加控件到未定价或远期交货业务布局
        future_layout.addLayout(future_button_layout)
        future_layout.addWidget(self.future_status_label)
        future_layout.addWidget(self.future_tab_widget)
        
        future_tab.setLayout(future_layout)
        
        # ====== 将三个主选项卡添加到顶层选项卡控件 ======
        self.main_tab_widget.addTab(risk_tab, "贸易经营风险指标")
        self.main_tab_widget.addTab(deposit_tab, "保证金")
        self.main_tab_widget.addTab(future_tab, "未定价或远期交货业务")
        
        # 导出按钮
        export_layout = QHBoxLayout()
        self.export_button = QPushButton("导出处理结果")
        self.export_button.clicked.connect(self.export_results)
        self.export_button.setEnabled(False)
        export_layout.addWidget(self.export_button)
        
        # 添加到主布局
        main_layout.addWidget(self.main_tab_widget)
        main_layout.addLayout(export_layout)
        
        # 设置中央窗口
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)
        
    def select_file(self):
        """打开文件选择对话框"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择Excel文件", "", "Excel Files (*.xlsx *.xls)")
        
        if file_path:
            self.file_label.setText(file_path)
            self.excel_processor.set_file_path(file_path)
            self.risk_status_label.setText(f"已选择文件: {file_path}")
    
    def process_excel(self):
        """处理Excel文件"""
        if not self.excel_processor.file_path:
            self.risk_status_label.setText("请先选择一个Excel文件")
            return
            
        self.risk_status_label.setText("正在读取Excel文件...")
        success = self.excel_processor.read_excel()
        
        if success:
            tables = self.excel_processor.get_tables()
            self.risk_status_label.setText(f"成功读取Excel文件中的{len(tables)}个表格")
            self.display_tables()
            self.process_overdue_button.setEnabled(True)
            self.process_delivery_button.setEnabled(True)
            self.process_inventory_button.setEnabled(True)
        else:
            self.risk_status_label.setText("读取Excel文件失败")
    
    def process_overdue_payment(self):
        """处理'一、逾期还款业务'表"""
        threshold = self.get_threshold()
        if threshold is None:
            return
            
        success = self.excel_processor.process_overdue_payment(threshold)
        
        if success:
            self.risk_status_label.setText(f"成功处理'一、逾期还款业务'表 (阈值: {threshold}万)")
            self.display_processed_table("一、逾期还款业务")
            self.export_button.setEnabled(True)
        else:
            self.risk_status_label.setText("处理'一、逾期还款业务'表失败")
    
    def process_overdue_delivery(self):
        """处理'二、付款逾期未到货(1)'表"""
        threshold = self.get_threshold()
        if threshold is None:
            return
            
        success = self.excel_processor.process_overdue_delivery(threshold)
        
        if success:
            self.risk_status_label.setText(f"成功处理'二、付款逾期未到货(1)'表 (阈值: {threshold}万)")
            self.display_processed_table("二、付款逾期未到货(1)")
            self.export_button.setEnabled(True)
        else:
            self.risk_status_label.setText("处理'二、付款逾期未到货(1)'表失败")
    
    def process_inventory(self):
        """处理'八、现货敞口90天及以上库存'表"""
        threshold = self.get_threshold()
        if threshold is None:
            return
            
        success = self.excel_processor.process_inventory(threshold)
        
        if success:
            self.risk_status_label.setText(f"成功处理'八、现货敞口90天及以上库存'表 (阈值: {threshold}万)")
            self.display_processed_table("八、现货敞口90天及以上库存")
            self.export_button.setEnabled(True)
        else:
            self.risk_status_label.setText("处理'八、现货敞口90天及以上库存'表失败")
    
    def process_deposit(self):
        """读取'保证金'sheet"""
        if not self.excel_processor.file_path:
            self.deposit_status_label.setText("请先选择一个Excel文件")
            return
            
        self.deposit_status_label.setText("正在读取'保证金'sheet...")
        success = self.excel_processor.read_deposit_sheet()
        
        if success:
            deposit_tables = self.excel_processor.get_deposit_tables()
            self.deposit_status_label.setText(f"成功读取'保证金'sheet中的{len(deposit_tables)}个表格")
            self.display_deposit_tables()
            self.process_deposit_ratio_button.setEnabled(True)
            self.process_deposit_inversion_button.setEnabled(True)
            self.export_button.setEnabled(True)
        else:
            self.deposit_status_label.setText("读取'保证金'sheet失败")
    
    def process_deposit_ratio(self):
        """处理'保证金比例低于合同约定比例'表"""
        # 显示阈值输入对话框
        dialog = ThresholdInputDialog(self, 300.0)  # 默认阈值 300
        result = dialog.exec()
        
        if result == QDialog.DialogCode.Accepted:
            threshold = dialog.get_threshold()
        else:
            return
        
        self.deposit_status_label.setText("正在处理'保证金比例低于合同约定比例'表...")
        success = self.excel_processor.process_deposit_ratio(threshold)
        
        if success:
            self.deposit_status_label.setText(f"成功处理'保证金比例低于合同约定比例'表 (阈值: {threshold}万)")
            self.display_processed_deposit_table("保证金比例低于合同约定比例")
            self.export_button.setEnabled(True)
        else:
            self.deposit_status_label.setText("处理'保证金比例低于合同约定比例'表失败")

    def process_deposit_inversion(self):
        """处理'未约定收保证金的锁定业务价格倒挂情况'表"""
        # 显示阈值输入对话框
        dialog = ThresholdInputDialog(self, 3000.0)  # 默认阈值 3000
        result = dialog.exec()
        
        if result == QDialog.DialogCode.Accepted:
            threshold = dialog.get_threshold()
        else:
            return
        
        self.deposit_status_label.setText("正在处理'未约定收保证金的锁定业务价格倒挂情况'表...")
        success = self.excel_processor.process_deposit_inversion(threshold)
        
        if success:
            self.deposit_status_label.setText(f"成功处理'未约定收保证金的锁定业务价格倒挂情况'表 (阈值: {threshold}万)")
            self.display_processed_deposit_table("未约定收保证金的锁定业务价格倒挂情况")
            self.export_button.setEnabled(True)
        else:
            self.deposit_status_label.setText("处理'未约定收保证金的锁定业务价格倒挂情况'表失败")
    
    def process_future(self):
        """读取'未定价或远期交货业务'sheet"""
        if not self.excel_processor.file_path:
            self.future_status_label.setText("请先选择一个Excel文件")
            return
            
        self.future_status_label.setText("正在读取'未定价或远期交货业务'sheet...")
        success = self.excel_processor.read_future_sheet()
        
        if success:
            future_tables = self.excel_processor.get_future_tables()
            self.future_status_label.setText(f"成功读取'未定价或远期交货业务'sheet中的{len(future_tables)}个表格")
            self.display_future_tables()
            self.process_future_summary_button.setEnabled(True)
            self.export_button.setEnabled(True)
        else:
            self.future_status_label.setText("读取'未定价或远期交货业务'sheet失败")

    def process_future_summary(self):
        """处理'汇总表（事业部及以上领导见本汇总表即可）'表"""
        # 显示阈值输入对话框
        dialog = ThresholdInputDialog(self, -30000000.0)  # 默认阈值 -30000000
        result = dialog.exec()
        
        if result == QDialog.DialogCode.Accepted:
            threshold = dialog.get_threshold()
        else:
            return
        
        self.future_status_label.setText("正在处理'汇总表（事业部及以上领导见本汇总表即可）'表...")
        success = self.excel_processor.process_future_summary(threshold)
        
        if success:
            self.future_status_label.setText(f"成功处理'汇总表（事业部及以上领导见本汇总表即可）'表 (阈值: {threshold}元)")
            self.display_processed_future_table("汇总表（事业部及以上领导见本汇总表即可）")
            self.export_button.setEnabled(True)
        else:
            self.future_status_label.setText("处理'汇总表（事业部及以上领导见本汇总表即可）'表失败")

    def export_results(self):
        """导出处理结果到Excel文件"""
        if not (self.excel_processor.processed_tables or self.excel_processor.deposit_tables or self.excel_processor.future_tables):
            self.risk_status_label.setText("没有可导出的处理结果")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存处理结果", "", "Excel Files (*.xlsx)")
            
        if not file_path:
            return
            
        if not file_path.endswith('.xlsx'):
            file_path += '.xlsx'
            
        try:
            # 创建一个ExcelWriter对象
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # 遍历所有处理过的表格并导出
                for table_name, table_data in self.excel_processor.processed_tables.items():
                    # 为不同类型的表生成适当的sheet名称
                    if "汇总表" in table_name:
                        sheet_name = "未定价汇总表"
                    elif "保证金比例低于合同约定比例" in table_name:
                        sheet_name = "保证金比例"
                    elif "未约定收保证金的锁定业务价格倒挂情况" in table_name:
                        sheet_name = "价格倒挂"
                    else:
                        # 贸易经营风险指标表格
                        sheet_name = table_name.split("、")[1] if "、" in table_name else table_name
                    
                    # 如果sheet名称太长，截取前31个字符（Excel的sheet名称长度限制）
                    sheet_name = sheet_name[:31]
                    # 导出到对应的sheet
                    table_data.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # 根据当前激活的标签页，设置相应的状态标签
            current_tab_index = self.main_tab_widget.currentIndex()
            if current_tab_index == 0:
                self.risk_status_label.setText(f"处理结果已导出到: {file_path}")
            elif current_tab_index == 1:
                self.deposit_status_label.setText(f"处理结果已导出到: {file_path}")
            else:
                self.future_status_label.setText(f"处理结果已导出到: {file_path}")
            
            # 询问是否打开导出的文件
            reply = QMessageBox.question(
                self, "导出成功", 
                f"处理结果已成功导出到:\n{file_path}\n\n是否立即打开该文件?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                # 使用默认程序打开Excel文件
                os.startfile(file_path)
                
        except Exception as e:
            # 根据当前激活的标签页，设置相应的状态标签
            current_tab_index = self.main_tab_widget.currentIndex()
            error_msg = f"导出失败: {str(e)}"
            if current_tab_index == 0:
                self.risk_status_label.setText(error_msg)
            elif current_tab_index == 1:
                self.deposit_status_label.setText(error_msg)
            else:
                self.future_status_label.setText(error_msg)
    
    def display_tables(self):
        """在选项卡中显示读取的表格"""
        # 清除现有选项卡
        self.risk_tab_widget.clear()
        
        # 获取所有表格
        tables = self.excel_processor.get_tables()
        
        for table_name, table_data in tables.items():
            # 创建表格视图
            table_view = QTableView()
            model = PandasModel(table_data)
            table_view.setModel(model)
            
            # 自动调整列宽以适应内容
            header = table_view.horizontalHeader()
            if header is not None:
                header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            
            # 将表格视图添加到新选项卡 - 截取表格名称中"、"后的内容，最多4个字符
            tab_name = table_name.split("、")[1][:4] if "、" in table_name else table_name[:4]
            self.risk_tab_widget.addTab(table_view, tab_name)
    
    def display_processed_table(self, table_name: str):
        """在新选项卡中显示处理后的表格"""
        processed_table = self.excel_processor.get_processed_table(table_name)
        
        if processed_table is None:
            return
            
        # 创建表格视图
        table_view = QTableView()
        model = PandasModel(processed_table)
        table_view.setModel(model)
        
        # 自动调整列宽以适应内容
        header = table_view.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        
        # 添加新选项卡，使用"处理后"作为标识
        tab_name = f"{table_name.split('、')[1][:4]}(处理后)" if "、" in table_name else f"{table_name[:4]}(处理后)"
        self.risk_tab_widget.addTab(table_view, tab_name)
        
        # 切换到新选项卡
        self.risk_tab_widget.setCurrentIndex(self.risk_tab_widget.count() - 1)

    def get_threshold(self):
        """获取用户输入的金额阈值"""
        dialog = ThresholdInputDialog(self, self.threshold)
        result = dialog.exec()
        
        if result == QDialog.DialogCode.Accepted:
            self.threshold = dialog.get_threshold()
            return self.threshold
        else:
            return None

    def display_deposit_tables(self):
        """在选项卡中显示读取的保证金表格"""
        # 获取所有保证金表格
        deposit_tables = self.excel_processor.get_deposit_tables()
        
        for table_name, table_data in deposit_tables.items():
            # 创建表格视图
            table_view = QTableView()
            model = PandasModel(table_data)
            table_view.setModel(model)
            
            # 自动调整列宽以适应内容
            header = table_view.horizontalHeader()
            if header is not None:
                header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            
            # 将表格视图添加到新选项卡 - 使用更友好的表名
            if "保证金比例低于合同约定比例" in table_name:
                tab_name = "保证金比例"
            elif "未约定收保证金的锁定业务价格倒挂情况" in table_name:
                tab_name = "价格倒挂"
            else:
                tab_name = "保证金_" + (table_name[:4] if len(table_name) > 4 else table_name)
            
            self.deposit_tab_widget.addTab(table_view, tab_name)
            
            # 切换到最新添加的选项卡
            self.deposit_tab_widget.setCurrentIndex(self.deposit_tab_widget.count() - 1)

    def display_processed_deposit_table(self, table_name: str):
        """在保证金选项卡中显示处理后的保证金表格"""
        processed_table = self.excel_processor.get_processed_table(table_name)
        
        if processed_table is None:
            return
            
        # 创建表格视图
        table_view = QTableView()
        model = PandasModel(processed_table)
        table_view.setModel(model)
        
        # 自动调整列宽以适应内容
        header = table_view.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        
        # 添加新选项卡，使用友好的名称
        if "保证金比例低于合同约定比例" in table_name:
            tab_name = "保证金比例(处理后)"
        elif "未约定收保证金的锁定业务价格倒挂情况" in table_name:
            tab_name = "价格倒挂(处理后)"
        else:
            tab_name = f"{table_name[:8]}(处理后)"
        
        self.deposit_tab_widget.addTab(table_view, tab_name)
        
        # 切换到新选项卡
        self.deposit_tab_widget.setCurrentIndex(self.deposit_tab_widget.count() - 1)

    def display_future_tables(self):
        """在选项卡中显示读取的未定价或远期交货业务表格"""
        # 获取所有未定价或远期交货业务表格
        future_tables = self.excel_processor.get_future_tables()
        
        for table_name, table_data in future_tables.items():
            # 创建表格视图
            table_view = QTableView()
            model = PandasModel(table_data)
            table_view.setModel(model)
            
            # 自动调整列宽以适应内容
            header = table_view.horizontalHeader()
            if header is not None:
                header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            
            # 将表格视图添加到新选项卡 - 使用简化的表格名称
            tab_name = "汇总表"
            self.future_tab_widget.addTab(table_view, tab_name)
            
            # 切换到最新添加的选项卡
            self.future_tab_widget.setCurrentIndex(self.future_tab_widget.count() - 1)

    def display_processed_future_table(self, table_name: str):
        """在未定价或远期交货业务选项卡中显示处理后的表格"""
        processed_table = self.excel_processor.get_processed_table(table_name)
        
        if processed_table is None:
            return
            
        # 创建表格视图
        table_view = QTableView()
        model = PandasModel(processed_table)
        table_view.setModel(model)
        
        # 自动调整列宽以适应内容
        header = table_view.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        
        # 添加新选项卡，使用友好的名称
        tab_name = "汇总表(处理后)"
        
        self.future_tab_widget.addTab(table_view, tab_name)
        
        # 切换到新选项卡
        self.future_tab_widget.setCurrentIndex(self.future_tab_widget.count() - 1)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ExcelProcessorApp()
    window.show()
    sys.exit(app.exec()) 