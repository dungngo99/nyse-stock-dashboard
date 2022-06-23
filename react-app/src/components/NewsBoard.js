import Moment from 'react-moment'
import 'moment-timezone';
import ClipLoader from "react-spinners/ClipLoader";

export default function NewsBoard(props) {
    const newsList = props.news

    if (newsList.length === 0) {
        return <ClipLoader color='red' size={50} />
    }

    return (
        <div>
            <p>Current news</p>
            <div style={{ "height": "85vh", "overflow": 'scroll' }}>
                {newsList.length !== 0 ? newsList['response'].map((item) => (
                    <div key={item['id']} >
                        <div className="card mb-3" >
                            <div className="row g-0">
                                <div className="col-md-4">
                                    <img
                                        src={item['thumbnailUrl']}
                                        alt="Trendy Pants and Shoes"
                                        className="img-fluid rounded-start hover-shadow"
                                    />
                                    <small className="text-muted">
                                        <Moment fromNow>{item['pubDate']}</Moment>
                                    </small>
                                </div>
                                <div className="col-md-8">
                                    <div className="card-body">
                                        <h5 className="card-title">{item['title']}</h5>
                                        <div className='d-flex justify-content-around'>
                                            {item['Url'] !== null ? <form action={item['Url']}>
                                                <input type="submit" value="Follow" />
                                            </form> : null}

                                            <div className="hover-overlay hover-shadow ripple">
                                                <button type="button" className="btn btn-dark btn-sm" data-mdb-ripple-color="dark">From: {item['provider']}</button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                )) : null}
            </div>
        </div>
    )
}