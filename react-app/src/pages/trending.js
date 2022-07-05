import React from 'react';
import {
    MDBContainer,
    MDBRow,
    MDBCol
} from 'mdb-react-ui-kit';
import { useSelector, useDispatch } from 'react-redux';
import {
    selectStock,
    getStockAsync,
    getMoversAsync,
    getNewsAsync
} from '../utils/redux';

import SearchBar from '../components/SearchBar.js';
import StockTable from '../components/StockTable.js';
import NewsBoard from '../components/NewsBoard.js';
import Movers from '../components/Movers.js';

export default function Trending() {
    const stockData = useSelector(selectStock)
    const dispatch = useDispatch()

    React.useEffect(() => {
        dispatch(getStockAsync())
        dispatch(getMoversAsync())
        dispatch(getNewsAsync())
    }, [dispatch]);

    return (
        <MDBContainer fluid >
            <SearchBar></SearchBar>
            <MDBRow>
                <MDBCol md='9' className='d-flex flex-column align-items-between'>
                    <StockTable trending={stockData['trending']}></StockTable>
                    <Movers top5={stockData['movers']} ></Movers>
                </MDBCol>

                <MDBCol md='3'>
                    <NewsBoard news={stockData['news']}></NewsBoard>
                </MDBCol>
            </MDBRow>
        </MDBContainer>
    )
};