import React, { useState } from 'react';
import {
    MDBContainer,
    MDBNavbar,
    MDBNavbarBrand,
    MDBNavbarToggler,
    MDBIcon,
    MDBNavbarNav,
    MDBNavbarItem,
    MDBBtn,
    MDBDropdown,
    MDBDropdownToggle,
    MDBDropdownMenu,
    MDBDropdownItem,
    MDBDropdownLink,
    MDBCollapse,
    MDBBadge
} from 'mdb-react-ui-kit';

let date = new Date();

export default function App() {
    const [showBasic, setShowBasic] = useState(false);
    const [currentTime, setCurrentTime] = useState(date.toLocaleDateString() + " " + date.toLocaleTimeString());

    const Ticker = () => { 
        date = new Date();
        setCurrentTime(date.toLocaleDateString() + " " + date.toLocaleTimeString());
    };
    setInterval(Ticker, 1000);

    return (
        <MDBNavbar expand='lg' light bgColor='light'>
            <MDBContainer fluid>
                <MDBNavbarBrand href='#'>Stockorgies</MDBNavbarBrand>

                <MDBNavbarToggler
                    aria-controls='navbarSupportedContent'
                    aria-expanded='false'
                    aria-label='Toggle navigation'
                    onClick={() => setShowBasic(!showBasic)}
                >
                    <MDBIcon icon='bars' fas />
                </MDBNavbarToggler>

                <MDBCollapse navbar show={showBasic}>
                    <MDBNavbarNav className='my-2 mb-2 mb-lg-0' fullWidth={true}>
                        <MDBNavbarItem>
                            <MDBDropdown>
                                <MDBDropdownToggle>Main menu</MDBDropdownToggle>
                                <MDBDropdownMenu>
                                    <MDBDropdownItem>
                                        <MDBDropdownLink href="#">Trending</MDBDropdownLink>
                                    </MDBDropdownItem>
                                    <MDBDropdownItem>
                                        <MDBDropdownLink href="#">Screener</MDBDropdownLink>
                                    </MDBDropdownItem>
                                    <MDBDropdownItem>
                                        <MDBDropdownLink href="#">Markets</MDBDropdownLink>
                                    </MDBDropdownItem>
                                    <MDBDropdownItem>
                                        <MDBDropdownLink href="#">Watchlist</MDBDropdownLink>
                                    </MDBDropdownItem>
                                    <MDBDropdownItem>
                                        <MDBDropdownLink href="#">News</MDBDropdownLink>
                                    </MDBDropdownItem>
                                    <MDBDropdownItem>
                                        <MDBDropdownLink href="#">Analyzers</MDBDropdownLink>
                                    </MDBDropdownItem>
                                </MDBDropdownMenu>
                            </MDBDropdown>
                        </MDBNavbarItem>

                        <MDBNavbarItem>
                            <h5><MDBBadge className='ms-2'>{currentTime}</MDBBadge></h5>
                        </MDBNavbarItem>

                    </MDBNavbarNav>

                    <form className='d-flex input-group w-auto'>
                        <input type='search' id="sb_input" className='form-control' placeholder='Ticker' aria-label='Search' />
                        <MDBBtn color='primary' size='sm'>Search</MDBBtn>
                    </form>
                </MDBCollapse>
            </MDBContainer>
        </MDBNavbar>
    );
}