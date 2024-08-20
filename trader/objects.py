from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class PositionData:
    adl: str
    availPos: str
    avgPx: str
    baseBal: str
    baseBorrowed: str
    baseInterest: str
    bePx: str
    bizRefId: str
    bizRefType: str
    cTime: str
    ccy: str
    clSpotInUseAmt: str
    closeOrderAlgo: List[Any]
    deltaBS: str
    deltaPA: str
    fee: str
    fundingFee: str
    gammaBS: str
    gammaPA: str
    idxPx: str
    imr: str
    instId: str
    instType: str
    interest: str
    last: str
    lever: str
    liab: str
    liabCcy: str
    liqPenalty: str
    liqPx: str
    margin: str
    markPx: str
    maxSpotInUseAmt: str
    mgnMode: str
    mgnRatio: str
    mmr: str
    notionalUsd: str
    optVal: str
    pendingCloseOrdLiabVal: str
    pnl: str
    pos: str
    posCcy: str
    posId: str
    posSide: str
    quoteBal: str
    quoteBorrowed: str
    quoteInterest: str
    realizedPnl: str
    spotInUseAmt: str
    spotInUseCcy: str
    thetaBS: str
    thetaPA: str
    tradeId: str
    uTime: str
    upl: str
    uplLastPx: str
    uplRatio: str
    uplRatioLastPx: str
    usdPx: str
    vegaBS: str
    vegaPA: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PositionData":
        return cls(**data)


def parse_position_data(data_packet: Dict[str, Any]) -> List[PositionData]:
    positions = data_packet.get("data", [])
    return [PositionData.from_dict(pos) for pos in positions]
