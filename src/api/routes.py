"""
API Routes for AutoGen Financial Analysis System
RESTful API endpoints
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from typing import List, Dict, Any, Optional
import asyncio
import logging
from datetime import datetime
from pathlib import Path

from .models import (
    AnalysisRequest, AnalysisResponse, AnalysisTask,
    SystemStatus, AnalysisType, AnalysisStatus, ValidationError,
    validate_analysis_request
)
from .websocket import WebSocketManager
from .task_manager import task_manager
from ..main import AutoGenFinancialAnalysisSystem
from ..monitoring.monitoring_system import SystemMonitor

# 全局变量
analysis_system = None
websocket_manager = None
system_monitor = None

api_routes = APIRouter()


async def run_analysis_task(task_id: str):
    """Run analysis task in background"""
    try:
        result = await task_manager.execute_task(task_id)

        # Broadcast result via WebSocket
        if websocket_manager:
            await websocket_manager.broadcast_task_update({
                "task_id": task_id,
                "status": "completed" if result.success else "failed",
                "result": result.result if result.success else None,
                "error": result.error if not result.success else None
            })

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Background task {task_id} failed: {str(e)}")

        # Update task status to failed
        await task_manager.update_task_status(
            task_id,
            AnalysisStatus.FAILED,
            None,
            f"Task execution failed: {str(e)}"
        )


@api_routes.on_event("startup")
async def startup_event():
    """启动事件"""
    global analysis_system, websocket_manager, system_monitor

    # 初始化系统
    analysis_system = AutoGenFinancialAnalysisSystem()
    await analysis_system.initialize()

    # 初始化任务管理器
    await task_manager.initialize(analysis_system)

    # 初始化WebSocket管理器
    websocket_manager = WebSocketManager()

    # 初始化监控系统
    system_monitor = SystemMonitor()
    system_monitor.start_monitoring()


@api_routes.on_event("shutdown")
async def shutdown_event():
    """关闭事件"""
    global analysis_system, websocket_manager, system_monitor

    if analysis_system:
        await analysis_system.cleanup()

    if websocket_manager:
        await websocket_manager.close()

    if system_monitor:
        system_monitor.stop_monitoring()


@api_routes.post("/analysis", response_model=Dict[str, Any])
async def create_analysis(
    request_data: Dict[str, Any],
    background_tasks: BackgroundTasks
):
    """创建分析任务"""
    try:
        # 验证请求数据
        request = validate_analysis_request(request_data)

        # 创建任务
        task = await task_manager.create_task(request)

        # 启动后台分析任务
        background_tasks.add_task(run_analysis_task, task.request.id)

        return {
            "request_id": task.request.id,
            "status": task.status.value,
            "message": "分析任务已创建"
        }

    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logging.error(f"创建分析任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail="内部服务器错误")


@api_routes.get("/analysis/{task_id}", response_model=Dict[str, Any])
async def get_analysis_status(task_id: str):
    """获取分析任务状态"""
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    return task.to_dict()


@api_routes.get("/analysis/{task_id}/result", response_model=Dict[str, Any])
async def get_analysis_result(task_id: str):
    """获取分析结果"""
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.status != AnalysisStatus.COMPLETED:
        raise HTTPException(status_code=400, detail="任务未完成")

    if not task.results:
        raise HTTPException(status_code=404, detail="分析结果不存在")

    return {
        "request_id": task_id,
        "results": task.results.to_dict() if hasattr(task.results, 'to_dict') else task.results,
        "export_files": task.export_files
    }


@api_routes.get("/analysis/{task_id}/export/{filename}")
async def download_export(task_id: str, filename: str):
    """下载导出文件"""
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if filename not in task.export_files:
        raise HTTPException(status_code=404, detail="文件不存在")

    file_path = Path("output") / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="文件不存在")

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/octet-stream'
    )


@api_routes.delete("/analysis/{task_id}")
async def cancel_analysis(task_id: str):
    """取消分析任务"""
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.status not in [AnalysisStatus.PENDING, AnalysisStatus.RUNNING]:
        raise HTTPException(status_code=400, detail="任务无法取消")

    await task_manager.update_task(
        task_id,
        status=AnalysisStatus.CANCELLED,
        completed_at=datetime.now()
    )

    return {"message": "任务已取消"}


@api_routes.get("/analysis", response_model=List[Dict[str, Any]])
async def list_analyses(
    status: Optional[AnalysisStatus] = None,
    limit: int = 50,
    offset: int = 0
):
    """获取分析任务列表"""
    # 简化实现，实际应该从数据库或缓存中获取
    tasks = list(task_manager.tasks.values())

    if status:
        tasks = [task for task in tasks if task.status == status]

    # 按创建时间排序
    tasks.sort(key=lambda x: x.created_at, reverse=True)

    # 分页
    tasks = tasks[offset:offset + limit]

    return [task.to_dict() for task in tasks]


@api_routes.get("/system/status", response_model=Dict[str, Any])
async def get_system_status():
    """获取系统状态"""
    try:
        # 获取活跃任务数
        active_tasks = await task_manager.get_active_tasks()

        # 获取系统资源使用情况
        system_resources = system_monitor.get_system_resources() if system_monitor else {}

        # 计算系统运行时间
        uptime = datetime.now().timestamp() - system_monitor.start_time if system_monitor else 0

        status = SystemStatus(
            status="healthy",
            uptime=uptime,
            active_tasks=len(active_tasks),
            completed_tasks=0,  # 需要从统计中获取
            failed_tasks=0,     # 需要从统计中获取
            system_resources=system_resources,
            api_version="1.0.0"
        )

        return status.to_dict()

    except Exception as e:
        logging.error(f"获取系统状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail="获取系统状态失败")


@api_routes.get("/system/metrics", response_model=Dict[str, Any])
async def get_system_metrics():
    """获取系统指标"""
    try:
        if not system_monitor:
            return {"message": "监控系统未启用"}

        metrics = system_monitor.get_metrics()
        return metrics

    except Exception as e:
        logging.error(f"获取系统指标失败: {str(e)}")
        raise HTTPException(status_code=500, detail="获取系统指标失败")


@api_routes.post("/quick-analysis")
async def quick_analysis(
    symbol: str,
    background_tasks: BackgroundTasks,
    analysis_type: AnalysisType = AnalysisType.QUICK
):
    """快速分析（同步返回）"""
    try:
        # 创建分析请求
        request = AnalysisRequest(
            symbols=[symbol],
            analysis_type=analysis_type
        )

        # 创建任务
        task = await task_manager.create_task(request)

        # 立即执行分析
        await run_analysis_task(task.request.id)

        # 获取结果
        task = await task_manager.get_task(task.request.id)
        if task.status == AnalysisStatus.COMPLETED:
            return task.to_dict()
        else:
            raise HTTPException(status_code=500, detail="分析失败")

    except Exception as e:
        logging.error(f"快速分析失败: {str(e)}")
        raise HTTPException(status_code=500, detail="分析失败")


@api_routes.post("/portfolio-analysis")
async def portfolio_analysis(
    request_data: Dict[str, Any],
    background_tasks: BackgroundTasks
):
    """投资组合分析"""
    try:
        # 验证请求数据
        request = validate_analysis_request(request_data)
        request.analysis_type = AnalysisType.PORTFOLIO

        if len(request.symbols) < 2:
            raise HTTPException(status_code=400, detail="投资组合分析至少需要2个股票")

        # 创建任务
        task = await task_manager.create_task(request)

        # 启动后台分析任务
        background_tasks.add_task(run_portfolio_analysis_task, task.request.id)

        return {
            "request_id": task.request.id,
            "status": task.status.value,
            "message": "投资组合分析任务已创建"
        }

    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logging.error(f"创建投资组合分析任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail="内部服务器错误")


@api_routes.get("/symbols/{symbol}/info")
async def get_symbol_info(symbol: str):
    """获取股票基本信息"""
    try:
        # 这里可以调用数据收集器获取股票信息
        # 简化实现
        return {
            "symbol": symbol,
            "name": f"{symbol} Inc.",
            "exchange": "NASDAQ",
            "currency": "USD",
            "last_updated": datetime.now().isoformat()
        }

    except Exception as e:
        logging.error(f"获取股票信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail="获取股票信息失败")


@api_routes.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket连接端点"""
    await websocket_manager.connect(websocket)
    try:
        while True:
            # 接收客户端消息
            data = await websocket.receive_json()

            if data.get("type") == "subscribe":
                # 订阅任务状态更新
                task_id = data.get("task_id")
                if task_id:
                    await websocket_manager.subscribe_task(websocket, task_id)
                    await websocket.send_json({
                        "type": "subscription_confirmed",
                        "task_id": task_id
                    })

            elif data.get("type") == "ping":
                # 心跳检测
                await websocket.send_json({
                    "type": "pong",
                    "timestamp": datetime.now().isoformat()
                })

    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)


# WebSocket管理器
class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.task_subscribers: Dict[str, List[WebSocket]] = {}
        self.logger = logging.getLogger(__name__)

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

        # 从任务订阅中移除
        for task_id, connections in self.task_subscribers.items():
            if websocket in connections:
                connections.remove(websocket)

    async def subscribe_task(self, websocket: WebSocket, task_id: str):
        if task_id not in self.task_subscribers:
            self.task_subscribers[task_id] = []
        self.task_subscribers[task_id].append(websocket)

    async def broadcast_task_update(self, task_id: str, update_data: Dict[str, Any]):
        if task_id in self.task_subscribers:
            message = {
                "type": "task_update",
                "task_id": task_id,
                "data": update_data,
                "timestamp": datetime.now().isoformat()
            }

            for connection in self.task_subscribers[task_id]:
                try:
                    await connection.send_json(message)
                except:
                    # 连接可能已断开，移除它
                    self.disconnect(connection)

    async def close(self):
        for connection in self.active_connections:
            await connection.close()
        self.active_connections.clear()
        self.task_subscribers.clear()


# 后台任务函数
async def run_analysis_task(task_id: str):
    """运行分析任务"""
    try:
        task = await task_manager.get_task(task_id)
        if not task:
            return

        # 更新任务状态
        await task_manager.update_task(
            task_id,
            status=AnalysisStatus.RUNNING,
            started_at=datetime.now(),
            progress=0.0
        )

        # 发送WebSocket更新
        if websocket_manager:
            await websocket_manager.broadcast_task_update(task_id, {
                "status": "running",
                "progress": 0.0
            })

        # 执行分析
        symbol = task.request.symbols[0]
        analysis_type = task.request.analysis_type

        progress_steps = 5
        current_step = 0

        # 步骤1: 数据收集
        current_step += 1
        await task_manager.update_task(task_id, progress=current_step / progress_steps * 100)
        if websocket_manager:
            await websocket_manager.broadcast_task_update(task_id, {
                "status": "running",
                "progress": current_step / progress_steps * 100,
                "step": "数据收集"
            })

        data = await analysis_system.data_collector.collect_comprehensive_data(symbol)

        # 步骤2: 财务分析
        current_step += 1
        await task_manager.update_task(task_id, progress=current_step / progress_steps * 100)
        if websocket_manager:
            await websocket_manager.broadcast_task_update(task_id, {
                "status": "running",
                "progress": current_step / progress_steps * 100,
                "step": "财务分析"
            })

        financial_metrics = analysis_system.financial_analyzer.calculate_comprehensive_metrics(data)

        # 步骤3: 风险分析
        current_step += 1
        await task_manager.update_task(task_id, progress=current_step / progress_steps * 100)
        if websocket_manager:
            await websocket_manager.broadcast_task_update(task_id, {
                "status": "running",
                "progress": current_step / progress_steps * 100,
                "step": "风险分析"
            })

        market_data = data.get('market_data', {}).get('price_history')
        risk_metrics = {}
        if market_data is not None:
            risk_metrics = analysis_system.risk_analyzer.calculate_comprehensive_risk_metrics(market_data)

        # 步骤4: 量化分析
        current_step += 1
        await task_manager.update_task(task_id, progress=current_step / progress_steps * 100)
        if websocket_manager:
            await websocket_manager.broadcast_task_update(task_id, {
                "status": "running",
                "progress": current_step / progress_steps * 100,
                "step": "量化分析"
            })

        quant_results = {}  # 简化实现

        # 步骤5: 生成报告和导出
        current_step += 1
        await task_manager.update_task(task_id, progress=current_step / progress_steps * 100)
        if websocket_manager:
            await websocket_manager.broadcast_task_update(task_id, {
                "status": "running",
                "progress": current_step / progress_steps * 100,
                "step": "生成报告"
            })

        # 创建分析结果
        from .models import AnalysisResult
        result = AnalysisResult(
            symbol=symbol,
            financial_metrics=financial_metrics.to_dict() if hasattr(financial_metrics, 'to_dict') else financial_metrics,
            risk_metrics=risk_metrics.to_dict() if hasattr(risk_metrics, 'to_dict') else risk_metrics,
            quant_metrics=quant_results,
            summary=analysis_system._generate_summary(financial_metrics, risk_metrics),
            recommendations=analysis_system._generate_recommendations(financial_metrics, risk_metrics),
            data_quality=data.get('data_quality', {}),
            analysis_date=datetime.now()
        )

        # 导出报告
        export_files = []
        if task.request.export_formats:
            output_path = Path("output")
            output_path.mkdir(exist_ok=True)
            await analysis_system._export_reports(
                {
                    'symbol': symbol,
                    'financial_metrics': result.financial_metrics,
                    'risk_metrics': result.risk_metrics,
                    'summary': result.summary,
                    'recommendations': result.recommendations
                },
                [f.value for f in task.request.export_formats],
                output_path,
                f"{symbol}_{task_id}"
            )

        # 完成任务
        await task_manager.update_task(
            task_id,
            status=AnalysisStatus.COMPLETED,
            results=result,
            export_files=export_files,
            progress=100.0,
            completed_at=datetime.now()
        )

        # 发送完成通知
        if websocket_manager:
            await websocket_manager.broadcast_task_update(task_id, {
                "status": "completed",
                "progress": 100.0
            })

    except Exception as e:
        logging.error(f"分析任务执行失败: {str(e)}")

        # 更新任务状态为失败
        await task_manager.update_task(
            task_id,
            status=AnalysisStatus.FAILED,
            error_message=str(e),
            completed_at=datetime.now()
        )

        # 发送失败通知
        if websocket_manager:
            await websocket_manager.broadcast_task_update(task_id, {
                "status": "failed",
                "error": str(e)
            })


async def run_portfolio_analysis_task(task_id: str):
    """运行投资组合分析任务"""
    try:
        task = await task_manager.get_task(task_id)
        if not task:
            return

        # 更新任务状态
        await task_manager.update_task(
            task_id,
            status=AnalysisStatus.RUNNING,
            started_at=datetime.now(),
            progress=0.0
        )

        # 执行投资组合分析
        symbols = task.request.symbols
        portfolio_weights = task.request.portfolio_weights

        result = await analysis_system.analyze_portfolio(
            symbols=symbols,
            portfolio_weights=portfolio_weights,
            export_formats=[f.value for f in task.request.export_formats],
            output_dir="output"
        )

        # 完成任务
        await task_manager.update_task(
            task_id,
            status=AnalysisStatus.COMPLETED,
            results=result,
            export_files=[],  # 实际应该从结果中获取
            progress=100.0,
            completed_at=datetime.now()
        )

    except Exception as e:
        logging.error(f"投资组合分析任务执行失败: {str(e)}")

        # 更新任务状态为失败
        await task_manager.update_task(
            task_id,
            status=AnalysisStatus.FAILED,
            error_message=str(e),
            completed_at=datetime.now()
        )
