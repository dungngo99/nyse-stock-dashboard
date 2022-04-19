import TickerBox from '../components/TickerBox.js';
import ClipLoader from "react-spinners/ClipLoader";

export default function Movers(props) {
  const tickers = ['AAPL', 'MSFT', 'AMZN', 'FB', 'COIN'];
  const top5 = props.top5;
  
  if (JSON.stringify(top5) === '{}') {
    return <ClipLoader color='red' size={50} />
  }

  return (
    <div className='d-flex flex-column'>
      <p className='align-self-start m-0 p-0' >Daily most active movers</p>
      <div className='d-flex justify-content-around'>
        {JSON.stringify(top5) !== '{}' ? tickers.map((item, index) => <TickerBox data={top5[item]} key={index} name={item}></TickerBox>) : null}
      </div>
    </div>
  )
}