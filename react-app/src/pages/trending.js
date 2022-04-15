import React, { useState } from 'react';
import {
    MDBContainer,
    MDBRow,
    MDBCol
} from 'mdb-react-ui-kit';
import TickerBox from '../components/TickerBox.js';
import SearchBar from '../components/SearchBar.js';
import StockTable from '../components/StockTable.js';
import api from '../utils/api'

export default function Trending() {
    const [trending, setTrending] = useState({})
    const [top5, setTop5] = useState({})
    const tickers = ['AAPL', 'MSFT', 'AMZN', 'FB', 'COIN'];

    React.useEffect(() => {
        api.get('trending', {
            method: 'GET'
        }).then(
            res => setTrending(res.data)
        ).catch(
            e => console.log(e)
        )

        api.get('movers', {
            method: 'GET'
        }).then(
            res => setTop5(res.data)
        ).catch(
            e => console.log(e)
        )
    }, []);
    
    return (
        <MDBContainer fluid >
            <SearchBar></SearchBar>
            <MDBRow>
                <MDBCol md='9'>
                    <StockTable trending={trending}></StockTable>
                    <div className='d-flex flex-column'>
                        <p className='align-self-start m-0 p-0' >Daily most active movers</p>
                        <div className='d-flex justify-content-around'>
                            {JSON.stringify(top5) !== '{}' ? tickers.map((item, index) => <TickerBox data={top5[item]} key={index} name={item}></TickerBox>): null}
                        </div>
                    </div>
                </MDBCol>

                <MDBCol md='3'>
                    <p>Current news</p>
                </MDBCol>
            </MDBRow>
        </MDBContainer>
    )
};